import logging
from collections import defaultdict

from hemera.common.utils.format_utils import bytes_to_hex_str, to_int_or_none
from hemera.common.utils.web3_utils import ZERO_ADDRESS
from hemera.indexer.domains.log import Log
from hemera.indexer.domains.token_transfer import ERC721TokenTransfer
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera.indexer.utils.multicall_hemera import Call
from hemera.indexer.utils.multicall_hemera.multi_call_helper import MultiCallHelper
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import (
    UniswapV4PoolFromToken,
    UniswapV4Token,
    UniswapV4TokenCurrentStatus,
    UniswapV4TokenDetail,
)
from hemera_udf.uniswap_v4.models.feature_uniswap_v4_pools import UniswapV4Pools
from hemera_udf.uniswap_v4.models.feature_uniswap_v4_tokens import UniswapV4Tokens
from hemera_udf.uniswap_v4.util import AddressManager

logger = logging.getLogger(__name__)


class ExportUniSwapV4TokensJob(FilterTransactionDataJob):
    dependency_types = [Log, ERC721TokenTransfer]
    output_types = [UniswapV4Token, UniswapV4TokenDetail, UniswapV4TokenCurrentStatus, UniswapV4PoolFromToken]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        config = kwargs["config"]["uniswap_v4_job"]
        jobs = config.get("jobs", [])
        self._address_manager = AddressManager(jobs)

        self.multi_call_helper = MultiCallHelper(self._web3, kwargs, logger)
        self._existing_tokens = self.get_existing_tokens()

    def get_filter(self):
        return TransactionFilterByLogs(
            [
                TopicSpecification(addresses=self._address_manager.position_token_address_list),
            ]
        )

    def _process(self, **kwargs):
        self.existing_pools = self.get_existing_pools()
        token_detail_list = []
        call_default_dict = defaultdict()

        burn_tokens_dict = {}
        # 1. When position_token_address tokens change
        erc721_token_transfers = [
            tt
            for tt in self._data_buff["erc721_token_transfer"]
            if tt.token_address in self._address_manager.position_token_address_list
        ]
        logs = self._data_buff["log"]

        erc721_token_transfers.sort(key=lambda x: x.block_number)

        for erc721_token_transfer in erc721_token_transfers:
            position_token_address = erc721_token_transfer.token_address
            token_id = erc721_token_transfer.token_id
            block_number = erc721_token_transfer.block_number
            block_timestamp = erc721_token_transfer.block_timestamp
            # If transferred to zero address, the token was burned
            if erc721_token_transfer.to_address == ZERO_ADDRESS:
                # Add token detail
                uniswap_v4_token_detail = UniswapV4TokenDetail(
                    position_token_address=position_token_address,
                    pool_address="",
                    token_id=token_id,
                    wallet_address=ZERO_ADDRESS,
                    liquidity=0,
                    block_number=block_number,
                    block_timestamp=block_timestamp,
                )
                token_detail_list.append(uniswap_v4_token_detail)
                burn_tokens_dict[position_token_address, token_id] = block_number
            # Add tokens that need to be queried via Ethereum calls
            else:
                # If position was already burned, no need to call
                if (position_token_address, token_id) not in burn_tokens_dict:
                    call_dict = {
                        "target": position_token_address,
                        "parameters": [token_id],
                        "block_number": block_number,
                        "user_defined_k": block_timestamp,
                    }
                    call_default_dict[position_token_address, token_id, block_number] = call_dict
                    
        # 2. Look for liquidity change events
        for log in logs:
            if log.address in self._address_manager.position_token_address_list:
                position_token_address = log.address
                block_number = log.block_number
                token_id = None
                # Different positions have different ABIs
                abi_module = self._address_manager.get_abi_by_position(position_token_address)
                # In V4, we need to check ModifyLiquidity event instead of increase/decrease liquidity events
                if log.topic0 == abi_module.MODIFY_LIQUIDITY_EVENT.get_signature():
                    decoded_data = abi_module.MODIFY_LIQUIDITY_EVENT.decode_log(log)
                    token_id = decoded_data.get("tokenId")  # Note: Actual contract may not include tokenId
                    # If tokenId is not found, may need to extract from other data
                    if token_id is None:
                        # Try to extract token_id from salt or other fields
                        # This needs to be adjusted based on actual contract implementation
                        pass

                if token_id:
                    call_dict = {
                        "target": position_token_address,
                        "parameters": [token_id],
                        "block_number": block_number,
                        "user_defined_k": log.block_timestamp,
                    }

                    key = (position_token_address, token_id)
                    not_burn_token_flag = key not in burn_tokens_dict
                    before_burn_token_flag = key in burn_tokens_dict and block_number < burn_tokens_dict[key]
                    if not_burn_token_flag or before_burn_token_flag:
                        call_default_dict[position_token_address, token_id, block_number] = call_dict

        # Remove tokens that have been burned
        keys_to_remove = []
        for key in call_default_dict.keys():
            position_token_address, token_id, block_number = key
            if (position_token_address, token_id) in burn_tokens_dict:
                if block_number >= burn_tokens_dict[position_token_address, token_id]:
                    keys_to_remove.append(key)
        for key in keys_to_remove:
            call_default_dict.pop(key)

        # Combine call lists from #1 and #2
        call_dict_list = list(call_default_dict.values())

        # ETH calls
        owner_call_list = []
        for call_dict in call_dict_list:
            abi_module = self._address_manager.get_abi_by_position(call_dict.get("target"))
            owner_call_list.append(Call(function_abi=abi_module.OWNER_OF_FUNCTION, **call_dict))
        self.multi_call_helper.execute_calls(owner_call_list)

        positions_call_list = []
        for call_dict in call_dict_list:
            abi_module = self._address_manager.get_abi_by_position(call_dict.get("target"))
            positions_call_list.append(Call(function_abi=abi_module.POSITIONS_FUNCTION, **call_dict))
        self.multi_call_helper.execute_calls(positions_call_list)

        positions_data_list = []
        # Decode data
        for owner_call, positions_call in zip(owner_call_list, positions_call_list):
            position_token_address = owner_call.target.lower()
            token_id = owner_call.parameters[0]

            block_number = owner_call.block_number
            block_timestamp = owner_call.user_defined_k

            positions = positions_call.returns

            if not owner_call.returns or not positions_call.returns:
                continue

            token0, token1, tick_lower, tick_upper, liquidity, fee = self.decode_positions_data(
                positions
            )
            data_dict = {
                "owner": owner_call.returns["owner"],
                "position_token_address": position_token_address,
                "token_id": token_id,
                "block_number": block_number,
                "block_timestamp": block_timestamp,
                "token0": token0,
                "token1": token1,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "liquidity": liquidity,
                "fee": fee,
            }
            positions_data_list.append(data_dict)
        
        # Get pool IDs
        self.get_pool_id_by_rpc(positions_data_list)

        for positions_data in positions_data_list:
            position_token_address = positions_data.get("position_token_address")
            token_id = positions_data.get("token_id")
            tick_lower = positions_data.get("tick_lower")
            tick_upper = positions_data.get("tick_upper")
            fee = positions_data.get("fee")
            block_number = positions_data.get("block_number")
            block_timestamp = positions_data.get("block_timestamp")
            liquidity = positions_data.get("liquidity")

            # In V4, pools are identified by PoolId (hash of PoolKey)
            # We need to use token0, token1, and fee to find the corresponding poolId
            pool_id = self.existing_pools.get(
                (
                    position_token_address,
                    positions_data.get("token0"),
                    positions_data.get("token1"),
                    positions_data.get("fee"),
                )
            )

            if pool_id:
                # Create Token record
                uniswap_v4_token = UniswapV4Token(
                    position_token_address=position_token_address,
                    token_id=token_id,
                    pool_address=pool_id,
                    tick_lower=tick_lower,
                    tick_upper=tick_upper,
                    fee=fee,
                    block_number=block_number,
                    block_timestamp=block_timestamp,
                )
                self._collect_domain(uniswap_v4_token)

                # Create pool-from-token record
                factory_address = self._address_manager.get_factory_by_position(position_token_address)
                if factory_address:
                    uniswap_v4_pool_from_token = UniswapV4PoolFromToken(
                        position_token_address=position_token_address,
                        factory_address=factory_address,
                        pool_address=pool_id,
                        token0_address=positions_data.get("token0"),
                        token1_address=positions_data.get("token1"),
                        fee=fee,
                        tick_spacing=0,  # May need to get from elsewhere
                        block_number=block_number,
                        block_timestamp=block_timestamp,
                    )
                    self._collect_domain(uniswap_v4_pool_from_token)

            wallet_address = positions_data.get("owner")
            uniswap_v4_token_detail = UniswapV4TokenDetail(
                position_token_address=position_token_address,
                pool_address=pool_id or "",
                token_id=token_id,
                wallet_address=wallet_address,
                liquidity=liquidity,
                block_number=block_number,
                block_timestamp=block_timestamp,
            )
            token_detail_list.append(uniswap_v4_token_detail)

        token_detail_list.sort(key=lambda t: t.block_number)
        current_token_detail_dict = {}
        for token_detail in token_detail_list:
            uniswap_token_current_status = UniswapV4TokenCurrentStatus(**vars(token_detail))
            current_token_detail_dict[
                uniswap_token_current_status.position_token_address, uniswap_token_current_status.token_id
            ] = uniswap_token_current_status
        
        self._collect_domains(token_detail_list)
        self._collect_domains(current_token_detail_dict.values())

    def get_pool_id_by_rpc(self, positions_data_list):
        # This function is similar to get_pool_address_by_rpc in v3, but adjusted for v4's PoolId mechanism
        # We need to get pool IDs through the factory contract
        missing_pool_positions_data_dict = {}
        for positions_data in positions_data_list:
            key = (
                positions_data.get("position_token_address"),
                positions_data.get("token0"),
                positions_data.get("token1"),
                positions_data.get("fee"),
            )
            pool_id = self.existing_pools.get(key)
            if not pool_id:
                missing_pool_positions_data_dict[key] = positions_data

        # If there are missing pool information, try to get via RPC
        call_list = []
        for positions_data in missing_pool_positions_data_dict.values():
            position_token_address = positions_data.get("position_token_address")
            factory_address = self._address_manager.get_factory_by_position(position_token_address)
            if not factory_address:
                continue
                
            abi_module = self._address_manager.get_abi_by_position(position_token_address)
            
            # Build call to get pool ID
            # Assumes there's a function that can get pool ID from token0, token1, and fee
            call = Call(
                target=factory_address,
                parameters=[
                    positions_data.get("token0"),
                    positions_data.get("token1"),
                    positions_data.get("fee"),
                ],
                function_abi=abi_module.GET_POOL_FUNCTION,  # Note: May need a different function
                block_number=positions_data.get("block_number"),
                user_defined_k=positions_data.get("block_timestamp"),
            )
            call_list.append(call)
        
        if call_list:
            self.multi_call_helper.execute_calls(call_list)
            
            # Process results
            for call in call_list:
                returns = call.returns
                if returns:
                    pool_id = returns.get("")
                    factory_address = call.target.lower()
                    position_token_address = self._address_manager.get_position_by_factory(factory_address)
                    
                    parameters = call.parameters
                    token0 = parameters[0]
                    token1 = parameters[1]
                    fee = parameters[2]
                    
                    # Update existing_pools
                    self.existing_pools[
                        (position_token_address, token0, token1, fee)
                    ] = pool_id

    def decode_positions_data(self, positions):
        """Extract needed fields from positions data"""
        token0 = positions.get("token0", "")
        token1 = positions.get("token1", "")
        tick_lower = to_int_or_none(positions.get("tickLower"))
        tick_upper = to_int_or_none(positions.get("tickUpper"))
        liquidity = to_int_or_none(positions.get("liquidity"))
        fee = to_int_or_none(positions.get("fee"))
        
        return token0, token1, tick_lower, tick_upper, liquidity, fee

    def get_existing_tokens(self):
        """Get existing token information"""
        # In real applications, this might need to load from database
        return {}

    def get_existing_pools(self):
        """Get existing pool information"""
        session = self._service.Session()
        try:
            pools_orm = session.query(UniswapV4Pools).all()
            existing_pools = {
                (
                    bytes_to_hex_str(p.position_token_address),
                    bytes_to_hex_str(p.token0_address),
                    bytes_to_hex_str(p.token1_address),
                    p.fee,
                ): bytes_to_hex_str(p.pool_address)
                for p in pools_orm
            }
        except Exception as e:
            logger.error(f"Failed to get existing pools: {e}")
            existing_pools = {}
        finally:
            session.close()
            
        return existing_pools 