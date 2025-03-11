import json
import logging

import hemera_udf.uniswap_v4.abi.uniswapv4_abi as uniswapv4_abi
from hemera.indexer.domains.log import Log
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4HookEvent, UniswapV4ETHSwapEvent
from hemera_udf.uniswap_v4.util import AddressManager

logger = logging.getLogger(__name__)


class ExportUniSwapV4HookJob(FilterTransactionDataJob):
    dependency_types = [Log]
    output_types = [UniswapV4HookEvent]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._service = kwargs["config"].get("db_service")
        config = kwargs["config"]["uniswap_v4_job"]
        jobs = config.get("jobs", [])
        self._address_manager = AddressManager(jobs)
        
        # List of known WETH hook addresses
        self.weth_hook_addresses = config.get("weth_hook_addresses", [])
        
        # Hook selectors to event types mapping
        self.selector_to_event_type = {
            uniswapv4_abi.BEFORE_INITIALIZE_SELECTOR: "before_initialize",
            uniswapv4_abi.AFTER_INITIALIZE_SELECTOR: "after_initialize",
            uniswapv4_abi.BEFORE_ADD_LIQUIDITY_SELECTOR: "before_add_liquidity",
            uniswapv4_abi.AFTER_ADD_LIQUIDITY_SELECTOR: "after_add_liquidity",
            uniswapv4_abi.BEFORE_REMOVE_LIQUIDITY_SELECTOR: "before_remove_liquidity",
            uniswapv4_abi.AFTER_REMOVE_LIQUIDITY_SELECTOR: "after_remove_liquidity",
            uniswapv4_abi.BEFORE_SWAP_SELECTOR: "before_swap",
            uniswapv4_abi.AFTER_SWAP_SELECTOR: "after_swap",
            uniswapv4_abi.BEFORE_DONATE_SELECTOR: "before_donate",
            uniswapv4_abi.AFTER_DONATE_SELECTOR: "after_donate",
        }

    def get_filter(self):
        # Add listeners for ETH send and receive events
        addresses = self._address_manager.hook_address_list + self.weth_hook_addresses
        
        return TransactionFilterByLogs(
            [
                TopicSpecification(
                    topics=[
                        uniswapv4_abi.HOOK_CALLED_EVENT.get_signature(),
                    ],
                    addresses=addresses,
                ),
            ]
        )

    def _process(self, **kwargs):
        self.process_hook_events()

    def process_hook_events(self):
        logs = self._data_buff[Log.type()]
        for log in logs:
            hook_event_dict = {}
            
            if log.topic0 == uniswapv4_abi.HOOK_CALLED_EVENT.get_signature():
                decoded_data = uniswapv4_abi.HOOK_CALLED_EVENT.decode_log(log)
                hook_address = decoded_data["hook"]
                caller_address = decoded_data["caller"]
                selector = decoded_data["selector"].hex()
                data = decoded_data["data"]
                
                # Get the factory address associated with this hook
                factory_address = self._address_manager.get_factory_by_hook(hook_address)
                
                if not factory_address:
                    logger.warning(f"Factory address not found for hook {hook_address}")
                    continue
                
                # Determine the event type from the selector
                event_type = self.selector_to_event_type.get(selector, "unknown")
                
                # Check if this is a WETH hook
                is_weth_hook = hook_address.lower() in [addr.lower() for addr in self.weth_hook_addresses]
                
                # Extract pool_id from the data if possible
                pool_id = None
                if len(data) >= 32:
                    # First 32 bytes might contain the pool ID in many hook calls
                    pool_id = "0x" + data[:32].hex()
                
                # Convert data to JSON string
                event_data = {
                    "caller": caller_address,
                    "selector": selector,
                    "data": data.hex() if data else None,
                }
                
                # If this is a WETH hook, try to parse ETH swap data
                if is_weth_hook and event_type in ["before_swap", "after_swap"]:
                    try:
                        # Parse WETH hook data to extract ETH swap information
                        # Note: Actual implementation needs to be adjusted based on the specific WETH hook implementation
                        eth_swap_dict = self._parse_eth_swap_data(
                            hook_address, 
                            pool_id or "", 
                            event_type, 
                            data, 
                            log.transaction_hash, 
                            log.log_index, 
                            log.block_number, 
                            log.block_timestamp
                        )
                        
                        if eth_swap_dict:
                            eth_swap_event = UniswapV4ETHSwapEvent(**eth_swap_dict)
                            self._collect_domain(eth_swap_event)
                    except Exception as e:
                        logger.error(f"Error parsing ETH swap data: {e}")
                
                hook_event_dict.update(
                    {
                        "hook_address": hook_address,
                        "factory_address": factory_address,
                        "pool_address": pool_id or "",  # Use pool_id if available, empty string otherwise
                        "hook_type": self._determine_hook_type(hook_address),
                        "event_type": event_type,
                        "event_data": json.dumps(event_data),
                        "transaction_hash": log.transaction_hash,
                        "log_index": log.log_index,
                        "block_number": log.block_number,
                        "block_timestamp": log.block_timestamp,
                    }
                )
            
            if hook_event_dict:
                uniswap_v4_hook_event = UniswapV4HookEvent(**hook_event_dict)
                self._collect_domain(uniswap_v4_hook_event)

    def _parse_eth_swap_data(self, hook_address, pool_id, event_type, data, tx_hash, log_index, block_number, block_timestamp):
        """
        Parse ETH swap data
        This function needs to be adjusted based on the actual WETH hook implementation
        """
        # This is a simplified implementation, actual parsing will depend on specific WETH hook data format
        # Example assumes data format includes: wallet address, ETH amount, token address, token amount, and swap direction
        try:
            # Actual parsing logic would be different, this is just an example
            wallet_address = "0x" + data[32:64].hex()[-40:]  # Example: extract wallet address from data
            
            # Assume ETH amount and token amount are at different positions
            eth_amount = int.from_bytes(data[64:96], byteorder='big', signed=True)
            token_address = "0x" + data[96:128].hex()[-40:]
            token_amount = int.from_bytes(data[128:160], byteorder='big', signed=True)
            
            # Determine swap direction
            is_eth_to_token = eth_amount > 0  # Positive ETH amount means ETH input, negative means ETH output
            
            return {
                "pool_address": pool_id,
                "eth_amount": eth_amount,
                "token_address": token_address,
                "token_amount": token_amount,
                "transaction_hash": tx_hash,
                "log_index": log_index,
                "wallet_address": wallet_address,
                "block_number": block_number,
                "block_timestamp": block_timestamp,
                "hook_address": hook_address,
                "is_eth_to_token": is_eth_to_token,
            }
        except Exception as e:
            logger.error(f"Failed to parse ETH swap data: {e}")
            return None
    
    def _determine_hook_type(self, hook_address):
        """
        Attempt to determine the type of hook
        """
        hook_address_lower = hook_address.lower()
        
        # Check if this is a WETH hook
        if hook_address_lower in [addr.lower() for addr in self.weth_hook_addresses]:
            return "weth"
        
        # Other known hook types
        known_hooks = {
            # These are examples, need to be populated based on actual data
            "0x...": "fee",
            "0x...": "dynamic_fee",
            "0x...": "limit_order",
        }
        
        return known_hooks.get(hook_address_lower, "unknown") 