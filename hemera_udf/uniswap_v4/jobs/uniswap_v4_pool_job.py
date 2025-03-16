import json
import logging

import hemera_udf.uniswap_v4.abi.uniswapv4_abi as uniswapv4_abi
from hemera.common.utils.format_utils import bytes_to_hex_str
from hemera.indexer.domains.log import Log
from hemera.indexer.jobs import FilterTransactionDataJob
from hemera.indexer.specification.specification import TopicSpecification, TransactionFilterByLogs
from hemera_udf.uniswap_v4.domains.feature_uniswap_v4 import UniswapV4Hook, UniswapV4Pool
from hemera_udf.uniswap_v4.models.feature_uniswap_v4_pools import UniswapV4Pools
from hemera_udf.uniswap_v4.util import AddressManager

logger = logging.getLogger(__name__)


class ExportUniSwapV4PoolJob(FilterTransactionDataJob):
    dependency_types = [Log]
    output_types = [UniswapV4Pool, UniswapV4Hook]
    able_to_reorg = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._service = kwargs["config"].get("db_service")
        config = kwargs["config"]["uniswap_v4_job"]
        jobs = config.get("jobs", [])
        self._address_manager = AddressManager(jobs)
        # self._existing_pools = self.get_existing_pools()

    def get_filter(self):
        return TransactionFilterByLogs(
            [
                TopicSpecification(
                    topics=[
                        abi_module.INITIALIZE_EVENT.get_signature()
                        for abi_module in self._address_manager.abi_modules_list
                    ],
                    addresses=self._address_manager.factory_address_list,
                ),
            ]
        )

    def _process(self, **kwargs):
        self.get_pools()

    def get_pools(self):
        logs = self._data_buff[Log.type()]
        for log in logs:
            pool_dict = {}
            hook_address = None
            pool_id = None

            # In Uniswap v4, the pool ID is derived from the PoolKey hash
            # We need to extract pool information from the Initialize event
            if log.topic0 == uniswapv4_abi.INITIALIZE_EVENT.get_signature():
                decoded_data = uniswapv4_abi.INITIALIZE_EVENT.decode_log(log)
                pool_id = decoded_data["id"]
                token0_address = decoded_data["currency0"]
                token1_address = decoded_data["currency1"]
                fee = decoded_data["fee"]
                tick_spacing = decoded_data["tickSpacing"]
                hook_address = decoded_data["hooks"]

                # In Uniswap v4, we don't have a direct pool address but we can use the pool_id
                # as a substitute for identification purposes

                position_token_address = self._address_manager.get_position_by_factory(log.address)
                if not position_token_address:
                    logger.warning(f"Position token address not found for factory {log.address}")
                    continue

                # Convert hook address to a JSON array for compatibility with our model
                pool_dict.update(
                    {
                        "factory_address": log.address,
                        "position_token_address": position_token_address,
                        "token0_address": token0_address,
                        "token1_address": token1_address,
                        "fee": fee,
                        "tick_spacing": tick_spacing,
                        "pool_address": pool_id,  # Using pool_id as a substitute for pool_address
                        "hook_address": hook_address,
                        "block_number": log.block_number,
                        "block_timestamp": log.block_timestamp,
                    }
                )

                # Process hook if present
                if hook_address and hook_address != "0x0000000000000000000000000000000000000000":
                    hook_type = self.determine_hook_type(hook_address)
                    hook_data = json.dumps({"permissions": hook_type})

                    hook_dict = {
                        "hook_address": hook_address,
                        "factory_address": log.address,
                        "pool_address": pool_id,
                        "hook_type": hook_type,
                        "hook_data": hook_data,
                        "block_number": log.block_number,
                        "block_timestamp": log.block_timestamp,
                    }
                    uniswap_v4_hook = UniswapV4Hook(**hook_dict)
                    self._collect_domain(uniswap_v4_hook)

            if pool_id and position_token_address:
                # self._existing_pools.add(pool_address)
                uniswap_v4_pool = UniswapV4Pool(**pool_dict)
                self._collect_domain(uniswap_v4_pool)

    def get_existing_pools(self):
        session = self._service.Session()
        try:
            existing_pools = set()
            pools_orm = session.query(UniswapV4Pools).all()
            for pool in pools_orm:
                existing_pools.add(bytes_to_hex_str(pool.pool_address))

        except Exception as e:
            print(e)
            raise e
        finally:
            session.close()

        return existing_pools

    def determine_hook_type(self, hook_address):
        # Convert hook address to integer
        addr_int = int(hook_address, 16)

        # Extract permissions from lowest bits
        permissions = []

        # Check each permission flag
        if addr_int & (1 << 13):
            permissions.append("before_initialize")
        if addr_int & (1 << 12):
            permissions.append("after_initialize")
        if addr_int & (1 << 11):
            permissions.append("before_add_liquidity")
        if addr_int & (1 << 10):
            permissions.append("after_add_liquidity")
        if addr_int & (1 << 9):
            permissions.append("before_remove_liquidity")
        if addr_int & (1 << 8):
            permissions.append("after_remove_liquidity")
        if addr_int & (1 << 7):
            permissions.append("before_swap")
        if addr_int & (1 << 6):
            permissions.append("after_swap")
        if addr_int & (1 << 5):
            permissions.append("before_donate")
        if addr_int & (1 << 4):
            permissions.append("after_donate")

        # Common hook types based on permissions
        if "before_swap" in permissions and "after_swap" in permissions:
            if addr_int & (1 << 3):  # beforeSwapReturnsDelta flag
                return "fee_hook"

        # Add more hook type detection logic

        return "unknown"
