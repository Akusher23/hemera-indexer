import json
import logging

import hemera_udf.uniswap_v4.abi.uniswapv4_abi as uniswapv4_abi
from hemera.common.utils.format_utils import bytes_to_hex_str
from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera.indexer.utils.multicall_hemera import Call
from hemera.indexer.utils.multicall_hemera.multi_call_helper import MultiCallHelper
from hemera_udf.token_price.domains import BlockTokenPrice
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import (
    UniswapV4PoolCurrentPrice,
    UniswapV4PoolFromSwapEvent,
    UniswapV4PoolPrice,
    UniswapV4SwapEvent,
)
from hemera_udf.uniswap_v4.models.feature_uniswap_v4_pools import UniswapV4Pools
from hemera_udf.uniswap_v4.util import AddressManager

logger = logging.getLogger(__name__)


class ExportUniSwapV4PoolPriceJob(FilterTransactionDataJob):
    dependency_types = [Transaction, BlockTokenPrice]
    output_types = [UniswapV4PoolPrice, UniswapV4PoolCurrentPrice, UniswapV4SwapEvent, UniswapV4PoolFromSwapEvent]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        config = kwargs["config"]["uniswap_v4_job"]
        jobs = config.get("jobs", [])
        self._pool_address = config.get("pool_address")
        self._address_manager = AddressManager(jobs)
        
        # WETH address and native ETH address
        self.weth_address = config.get("weth_address", "").lower()
        self.eth_address = uniswapv4_abi.ETH_ADDRESS

        self.multi_call_helper = MultiCallHelper(self._web3, kwargs, logger)
        self.pools_requested_by_rpc = set()
        # self.token_decimals_map = {}

        stable_tokens_config = kwargs["config"].get("export_block_token_price_job", {})

        self.stable_tokens = stable_tokens_config
        self._exist_pools = self.get_existing_pools()
        self.tokens = {}  # This needs to be populated in real applications, containing token address to decimals mapping

    def get_filter(self):
        address_list = self._pool_address if self._pool_address else []

        return TransactionFilterByLogs(
            [
                TopicSpecification(
                    topics=[uniswapv4_abi.SWAP_EVENT.get_signature()],
                    addresses=address_list,
                ),
            ]
        )

    def change_block_token_prices_to_dict(self):
        symbol_address_dict = {symbol: address for address, symbol in self.stable_tokens.items()}
        token_prices_dict = {}

        block_token_prices = self._data_buff[BlockTokenPrice.type()]
        for token_price in block_token_prices:
            address = symbol_address_dict.get(token_price.token_symbol)
            if address:
                block_number = token_price.block_number
                token_prices_dict[address, block_number] = token_price.token_price

        return token_prices_dict

    def get_missing_pools_by_rpc(self):
        # This function needs to get pool information not in self._exist_pools
        # In Uniswap v4, we may need to handle this differently
        # because pool IDs are generated through hashing rather than simple addresses
        missing_pool_address_dict = {}
        transactions = self._data_buff["transaction"]

        for transaction in transactions:
            logs = transaction.receipt.logs
            for log in logs:
                if log.topic0 == uniswapv4_abi.SWAP_EVENT.get_signature() and log.address not in self._exist_pools:
                    if log.address not in self.pools_requested_by_rpc:
                        # Parse pool ID from SWAP_EVENT
                        decoded_data = uniswapv4_abi.SWAP_EVENT.decode_log(log)
                        pool_id = decoded_data["id"]
                        
                        # If this is a pool we don't know about, try to get its information
                        # This is a simplified implementation, actual code may need a different approach
                        self.pools_requested_by_rpc.add(pool_id)
                        
                        # Try to call contract method to get pool information
                        # In Uniswap V4, we need to use low-level storage access via extsload
                        # The pool data is stored in the pools mapping
                        call_dict = {
                            "target": log.address,  # PoolManager contract address
                            "function_name": "extsload",  # Use extsload to directly access storage
                            "function_signature": "function extsload(bytes32) view returns (bytes32)",
                            "args": [pool_id],  # The pool ID is used as the slot key
                            "block_number": log.block_number,
                        }
                        missing_pool_address_dict[pool_id] = call_dict

        # If there are missing pool information, try to get via RPC
        # if missing_pool_address_dict:
        #     calls = []
        #     for pool_id, call_dict in missing_pool_address_dict.items():
        #         calls.append(
        #             Call(
        #                 target=call_dict["target"],
        #                 function_abi=call_dict["function_name"],
        #                 parameters=call_dict["args"],
        #                 block_number=call_dict["block_number"],
        #             )
        #         )
        #
        #     # Execute batch calls
        #     try:
        #         results = self.multi_call_helper.aggregate(calls)
        #         # Process results
        #         for i, (pool_id, call_dict) in enumerate(missing_pool_address_dict.items()):
        #             if results[i]:
        #                 # The raw storage data will need to be decoded properly
        #                 raw_pool_data = results[i]
        #
        #                 logger.info(f"Retrieved raw pool data for {pool_id}: {raw_pool_data}")
        #
        #                 # Without knowing the exact storage layout, it's difficult to decode properly
        #                 # For now, we'll create a partial record with what we know
        #                 factory_address = call_dict["target"]  # The PoolManager address
        #                 position_token_address = self._address_manager.get_position_by_factory(factory_address)
        #
        #                 # Create a placeholder record until we can properly decode the storage
        #                 self._exist_pools[pool_id] = {
        #                     "token0_address": None,  # Need proper decoding
        #                     "token1_address": None,  # Need proper decoding
        #                     "fee": None,             # Need proper decoding
        #                     "tick_spacing": None,    # Need proper decoding
        #                     "hooks": None,           # Need proper decoding
        #                     "factory_address": factory_address,
        #                     "position_token_address": position_token_address,
        #                 }
        #
        #                 # Alternative approach: query Initialize events to get pool creation data
        #                 # This would be more reliable but requires access to event logs
        #                 logger.info(f"Added placeholder for pool {pool_id}, consider querying Initialize events")
        #     except Exception as e:
        #         logger.error(f"Failed to get pool info: {e}")

    def _process(self, **kwargs):
        token_prices_dict = self.change_block_token_prices_to_dict()

        if not self._pool_address:
            self.get_missing_pools_by_rpc()

        transactions = self._data_buff["transaction"]
        current_price_dict = {}
        price_dict = {}

        for transaction in transactions:
            logs = transaction.receipt.logs
            for log in logs:
                if log.topic0 == uniswapv4_abi.SWAP_EVENT.get_signature():
                    decoded_data = uniswapv4_abi.SWAP_EVENT.decode_log(log)
                    pool_id = decoded_data["id"]
                    
                    # Check if this pool is in our known pools
                    if pool_id in self._exist_pools and self._exist_pools[pool_id].get("token0_address"):
                        pool_data = self._exist_pools[pool_id].copy()
                        factory_address = pool_data.pop("factory_address")
                        key_data_dict = {
                            "tick": decoded_data["tick"],
                            "sqrt_price_x96": decoded_data["sqrtPriceX96"],
                            "block_number": log.block_number,
                            "block_timestamp": log.block_timestamp,
                            "pool_address": pool_id,  # Use pool_id as identifier
                        }
                        
                        token0_address = pool_data.get("token0_address")
                        token1_address = pool_data.get("token1_address")

                        # Check if this involves ETH/WETH
                        involves_eth = False
                        if token0_address.lower() == self.weth_address.lower() or token1_address.lower() == self.weth_address.lower():
                            involves_eth = True
                            # If using WETH, we can treat it as ETH
                            if token0_address.lower() == self.weth_address.lower():
                                token0_address = self.eth_address
                            if token1_address.lower() == self.weth_address.lower():
                                token1_address = self.eth_address

                        tokens0 = self.tokens.get(token0_address)
                        tokens1 = self.tokens.get(token1_address)

                        decimals0 = tokens0.get("decimals") if tokens0 else None
                        decimals1 = tokens1.get("decimals") if tokens1 else None

                        amount0 = decoded_data["amount0"]
                        amount1 = decoded_data["amount1"]

                        amount0_abs = abs(amount0)
                        amount1_abs = abs(amount1)

                        decimals_conditions = decimals0 and decimals1

                        # Price calculation logic, similar to v3
                        if token0_address in self.stable_tokens and decimals_conditions:
                            token0_price = token_prices_dict.get((token0_address, log.block_number))
                            amount_usd = amount0_abs / 10**decimals0 * token0_price if token0_price else None
                            token1_price = amount_usd / (amount1_abs / 10**decimals1) if amount1_abs > 0 and amount_usd else None
                        elif token1_address in self.stable_tokens and decimals_conditions:
                            token1_price = token_prices_dict.get((token1_address, log.block_number))
                            amount_usd = amount1_abs / 10**decimals1 * token1_price if token1_price else None
                            token0_price = amount_usd / (amount0_abs / 10**decimals0) if amount0_abs > 0 and amount_usd else None
                        else:
                            token0_price = None
                            token1_price = None
                            amount_usd = None

                        # Create price record
                        pool_price_item = UniswapV4PoolPrice(
                            **key_data_dict,
                            factory_address=factory_address,
                            token0_price=token0_price,
                            token1_price=token1_price,
                        )
                        price_dict[pool_id, log.block_number] = pool_price_item
                        current_price_dict[pool_id] = UniswapV4PoolCurrentPrice(**vars(pool_price_item))

                        # Create swap event record
                        self._collect_domain(
                            UniswapV4SwapEvent(
                                transaction_hash=log.transaction_hash,
                                transaction_from_address=transaction.from_address,
                                log_index=log.log_index,
                                sender=decoded_data["sender"],
                                recipient=None,  # v4 SWAP event doesn't include recipient
                                amount0=amount0,
                                amount1=amount1,
                                liquidity=decoded_data["liquidity"],
                                **key_data_dict,
                                **pool_data,
                                token0_price=token0_price,
                                token1_price=token1_price,
                                amount_usd=amount_usd,
                                hook_data=None,  # May need to process hookData here
                                is_eth_swap=involves_eth,  # Mark if involves ETH
                            ),
                        )
                        
                        # Also create and collect pool info from swap event
                        # This ensures we capture pools discovered through swap events
                        self._collect_domain(
                            UniswapV4PoolFromSwapEvent(
                                pool_address=pool_id,
                                position_token_address=pool_data.get("position_token_address"),
                                factory_address=factory_address,
                                token0_address=token0_address,
                                token1_address=token1_address,
                                fee=pool_data.get("fee"),
                                tick_spacing=pool_data.get("tick_spacing"),
                                hooks=pool_data.get("hooks"),
                                block_number=log.block_number,
                                block_timestamp=log.block_timestamp,
                            ),
                        )
                    else:
                        pass
                        # Handle unknown pool_id
                        logger.info(f"Discovered new pool from swap event: {pool_id}")
                        
                        # Try to determine which factory this pool belongs to via RPC
                        # We'll query each factory to find which one this pool belongs to
                        self.get_pool_info_by_rpc(pool_id, log.block_number, log.block_timestamp, decoded_data)
                        
                        # After RPC query, check if we now have the pool information
                        if pool_id in self._exist_pools:
                            # Process the swap event again now that we have the pool info
                            # This is a bit inefficient but ensures consistent processing
                            logger.info(f"Pool {pool_id} added to known pools, processing swap event")
                            if log.topic0 == uniswapv4_abi.SWAP_EVENT.get_signature():
                                decoded_data = uniswapv4_abi.SWAP_EVENT.decode_log(log)
                                pool_id = decoded_data["id"]
                                
                                if pool_id in self._exist_pools:
                                    # Process pool data (now it exists)
                                    # ... existing processing code for known pools ...
                                    pool_data = self._exist_pools[pool_id].copy()
                                    factory_address = pool_data.pop("factory_address")
                                    key_data_dict = {
                                        "tick": decoded_data["tick"],
                                        "sqrt_price_x96": decoded_data["sqrtPriceX96"],
                                        "block_number": log.block_number,
                                        "block_timestamp": log.block_timestamp,
                                        "pool_address": pool_id,
                                    }
                                    
                                    token0_address = pool_data.get("token0_address")
                                    token1_address = pool_data.get("token1_address")
                                    
                                    # Create pool info from swap event with complete data
                                    self._collect_domain(
                                        UniswapV4PoolFromSwapEvent(
                                            pool_address=pool_id,
                                            position_token_address=pool_data.get("position_token_address"),
                                            factory_address=factory_address,
                                            token0_address=token0_address,
                                            token1_address=token1_address,
                                            fee=pool_data.get("fee"),
                                            tick_spacing=pool_data.get("tick_spacing"),
                                            hook_address=pool_data.get("hooks"),
                                            block_number=log.block_number,
                                            block_timestamp=log.block_timestamp,
                                        ),
                                    )

        # Collect all price records
        self._collect_domains(price_dict.values())
        self._collect_domains(list(current_price_dict.values()))

    def get_existing_pools(self):
        session = self._service.Session()
        try:
            pools_orm = session.query(UniswapV4Pools).all()
            existing_pools = {
                bytes_to_hex_str(p.pool_address): {
                    "token0_address": bytes_to_hex_str(p.token0_address),
                    "token1_address": bytes_to_hex_str(p.token1_address),
                    "position_token_address": bytes_to_hex_str(p.position_token_address),
                    "factory_address": bytes_to_hex_str(p.factory_address),
                }
                for p in pools_orm
            }
        except Exception as e:
            logger.error(f"Failed to get existing pools: {e}")
            existing_pools = {}
        finally:
            session.close()

        return existing_pools 

    def get_pool_info_by_rpc(self, pool_id, block_number, block_timestamp, decoded_data=None):
        """
        Fetch pool information using RPC calls for unknown pool IDs
        Similar to get_pool_id_by_rpc in UniswapV4TokenJob, but works in reverse:
        Here we have the pool ID and need to find token0, token1, fee, etc.
        """
        # We need to query additional information for this pool
        call_list = []
        
        # Try with each known factory
        for factory_address in self._address_manager.factory_address_list:
            position_token_address = self._address_manager.get_position_by_factory(factory_address)
            if not position_token_address:
                continue
                
            abi_module = self._address_manager.get_abi_by_factory(factory_address)
            if not abi_module:
                continue
            
            # Get StateView contract address if available
            state_view_address = self._address_manager.get_state_view_address(factory_address)
            if state_view_address:
                # Using StateView contract to query pool information
                # We need to define the StateView function ABI
                # For this example, we'll assume there's a function that can get pool key from pool ID
                try:
                    # Build call to get pool key information
                    getPoolKey_abi = {
                        "inputs": [{"internalType": "bytes32", "name": "poolId", "type": "bytes32"}],
                        "name": "getPoolKey",
                        "outputs": [
                            {"internalType": "address", "name": "currency0", "type": "address"},
                            {"internalType": "address", "name": "currency1", "type": "address"},
                            {"internalType": "uint24", "name": "fee", "type": "uint24"},
                            {"internalType": "int24", "name": "tickSpacing", "type": "int24"},
                            {"internalType": "address", "name": "hooks", "type": "address"}
                        ],
                        "stateMutability": "view",
                        "type": "function"
                    }
                    
                    from hemera.common.utils.abi_code_utils import Function
                    getPoolKey_function = Function(getPoolKey_abi)
                    
                    # Create call to StateView contract
                    call = Call(
                        target=state_view_address,
                        parameters=[pool_id],
                        function_abi=getPoolKey_function,
                        block_number=block_number,
                        user_defined_k=block_timestamp,
                    )
                    call_list.append(call)
                    
                except Exception as e:
                    logger.error(f"Failed to create RPC call for pool {pool_id}: {e}")
                    continue
        
        # Execute the calls
        if call_list:
            try:
                self.multi_call_helper.execute_calls(call_list)
                
                # Process results
                for call in call_list:
                    returns = call.returns
                    if returns:
                        # Extract pool information from returns
                        token0_address = returns.get("currency0")
                        token1_address = returns.get("currency1")
                        fee = returns.get("fee")
                        tick_spacing = returns.get("tickSpacing")
                        hook_address = returns.get("hooks")
                        
                        # Get factory and position token info
                        state_view_address = call.target.lower()
                        factory_address = None
                        position_token_address = None
                        
                        # Find which factory this state view belongs to
                        for f_addr in self._address_manager.factory_address_list:
                            if self._address_manager.get_state_view_address(f_addr) == state_view_address:
                                factory_address = f_addr
                                position_token_address = self._address_manager.get_position_by_factory(f_addr)
                                break
                        
                        if factory_address and position_token_address:
                            # Store the pool information
                            self._exist_pools[pool_id] = {
                                "token0_address": token0_address,
                                "token1_address": token1_address,
                                "fee": fee,
                                "tick_spacing": tick_spacing,
                                "hooks": hook_address,
                                "factory_address": factory_address,
                                "position_token_address": position_token_address,
                            }
                            logger.info(f"Successfully retrieved pool info for {pool_id} via RPC")
                            return
            except Exception as e:
                logger.error(f"Failed to execute RPC calls for pool {pool_id}: {e}")
        
        # If we couldn't get complete information, create a partial record with what we know
        if decoded_data and "tick" in decoded_data and "sqrtPriceX96" in decoded_data:
            for factory_address in self._address_manager.factory_address_list:
                position_token_address = self._address_manager.get_position_by_factory(factory_address)
                if position_token_address:
                    # Create pool info with partial data
                    self._exist_pools[pool_id] = {
                        "factory_address": factory_address,
                        "position_token_address": position_token_address,
                        "token0_address": None,  # Unknown
                        "token1_address": None,  # Unknown
                        "fee": None,  # Unknown
                        "tick_spacing": None,  # Unknown
                        "hooks": None,  # Unknown
                    }
                    logger.info(f"Added partial pool data for {pool_id} to known pools (factory: {factory_address})")
                    return 