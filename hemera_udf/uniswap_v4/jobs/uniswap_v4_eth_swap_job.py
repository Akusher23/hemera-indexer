import logging

import hemera_udf.uniswap_v4.abi.uniswapv4_abi as uniswapv4_abi
from hemera.indexer.domains.log import Log
from hemera.indexer.domains.transaction import Transaction
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4ETHSwapEvent
from hemera_udf.uniswap_v4.util import AddressManager

logger = logging.getLogger(__name__)


class ExportUniSwapV4ETHSwapJob(FilterTransactionDataJob):
    """
    Specialized job for handling ETH swaps in Uniswap V4
    This job tracks ETH swaps by monitoring Ethereum transactions and WETH hook events
    """
    dependency_types = [Log, Transaction]
    output_types = [UniswapV4ETHSwapEvent]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        config = kwargs["config"]["uniswap_v4_job"]
        jobs = config.get("jobs", [])
        self._address_manager = AddressManager(jobs)
        
        # Get WETH hook and WETH token addresses
        self.weth_hook_addresses = config.get("weth_hook_addresses", [])
        self.weth_token_address = config.get("weth_address", "").lower()
        
        # ETH address (zero address)
        self.eth_address = uniswapv4_abi.ETH_ADDRESS

    def get_filter(self):
        # Monitor WETH hook events
        return TransactionFilterByLogs(
            [
                TopicSpecification(
                    topics=[
                        uniswapv4_abi.HOOK_CALLED_EVENT.get_signature(),
                    ],
                    addresses=self.weth_hook_addresses,
                ),
            ]
        )

    def _process(self, **kwargs):
        # Process WETH hook events to identify ETH swaps
        self.process_eth_swaps()
        
        # Can add more complex logic, such as checking if transaction value matches ETH swap amount
        self.verify_eth_transfers()

    def process_eth_swaps(self):
        """Process swap events involving ETH"""
        logs = self._data_buff[Log.type()]
        
        # Sort by block number and transaction hash to maintain event order
        logs.sort(key=lambda x: (x.block_number, x.transaction_hash, x.log_index))
        
        for log in logs:
            if log.topic0 == uniswapv4_abi.HOOK_CALLED_EVENT.get_signature() and log.address in self.weth_hook_addresses:
                decoded_data = uniswapv4_abi.HOOK_CALLED_EVENT.decode_log(log)
                hook_address = decoded_data["hook"]
                selector = decoded_data["selector"].hex()
                data = decoded_data["data"]
                
                # Check if this is a hook call related to swapping
                # This needs to be adjusted based on the specific WETH hook implementation
                if selector in [uniswapv4_abi.BEFORE_SWAP_SELECTOR, uniswapv4_abi.AFTER_SWAP_SELECTOR]:
                    try:
                        # Parse ETH swap data
                        eth_swap_dict = self._parse_eth_swap_data(
                            hook_address, 
                            log.transaction_hash, 
                            log.log_index, 
                            data, 
                            log.block_number, 
                            log.block_timestamp
                        )
                        
                        if eth_swap_dict:
                            eth_swap_event = UniswapV4ETHSwapEvent(**eth_swap_dict)
                            self._collect_domain(eth_swap_event)
                    except Exception as e:
                        logger.error(f"Error parsing ETH swap data: {e}")

    def verify_eth_transfers(self):
        """Verify if transaction value matches ETH swap amount"""
        transactions = self._data_buff["transaction"]
        # Here you can add logic to compare transaction value with ETH amount extracted from hook events
        # This can help verify if our ETH swap tracking is accurate
        # Implementation depends on specific requirements and data structures

    def _parse_eth_swap_data(self, hook_address, tx_hash, log_index, data, block_number, block_timestamp):
        """
        Parse ETH swap data
        This function needs to be adjusted based on the actual WETH hook implementation
        """
        # This is a simplified implementation, actual parsing will depend on specific WETH hook data format
        try:
            # Extract pool ID - assumed to be in first 32 bytes
            pool_id = "0x" + data[:32].hex() if len(data) >= 32 else ""
            
            # Extract wallet address - assumed position
            wallet_address = "0x" + data[32:64].hex()[-40:] if len(data) >= 64 else ""
            
            # Extract ETH amount and token address/amount - assumed positions
            # Note: Actual positions and format need to be determined based on actual WETH hook implementation
            eth_amount = int.from_bytes(data[64:96], byteorder='big', signed=True) if len(data) >= 96 else 0
            token_address = "0x" + data[96:128].hex()[-40:] if len(data) >= 128 else ""
            token_amount = int.from_bytes(data[128:160], byteorder='big', signed=True) if len(data) >= 160 else 0
            
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