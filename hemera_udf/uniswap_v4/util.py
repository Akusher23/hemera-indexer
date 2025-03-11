import logging

import hemera_udf.uniswap_v4.abi.uniswapv4_abi as uniswapv4_abi

logger = logging.getLogger(__name__)


class BiDirectionalDict:
    def __init__(self, initial_dict=None):
        self.forward = initial_dict or {}
        self.backward = {v: k for k, v in self.forward.items()}

    def add(self, key, value):
        self.forward[key] = value
        self.backward[value] = key

    def get_forward(self, key):
        return self.forward.get(key)

    def get_backward(self, value):
        return self.backward.get(value)


class AddressManager:
    def __init__(self, jobs):
        # for filter topics
        self.abi_modules_list = []
        self.factory_address_list = []
        self.position_token_address_list = []
        self.hook_address_list = []

        self.factory_to_position = {}
        self.position_to_factory = {}
        self.hook_to_factory = {}

        self._build_mappings(jobs)

    def _build_mappings(self, jobs):
        for job in jobs:
            type_str = job.get("type")
            factory_address = job.get("factory_address").lower()
            position_token_address = job.get("position_token_address").lower()
            hook_addresses = job.get("hook_addresses", [])
            state_view_address = job.get("state_view_address", "").lower() if job.get("state_view_address") else None
            
            if hook_addresses:
                for hook_address in hook_addresses:
                    if hook_address.lower() not in self.hook_address_list:
                        self.hook_address_list.append(hook_address.lower())
                    self.hook_to_factory[hook_address.lower()] = factory_address

            if factory_address not in self.factory_address_list:
                self.factory_address_list.append(factory_address)

            if position_token_address not in self.position_token_address_list:
                self.position_token_address_list.append(position_token_address)

            abi_module = self._get_abi_module(type_str)
            if abi_module not in self.abi_modules_list:
                self.abi_modules_list.append(abi_module)

            if not factory_address or not position_token_address or not abi_module:
                raise ValueError("Factory address, position token address, and ABI module are required")

            self.factory_to_position[factory_address] = {
                "position_token_address": position_token_address,
                "type": type_str,
                "abi_module": abi_module,
                "hook_addresses": hook_addresses,
                "state_view_address": state_view_address,
            }
            self.position_to_factory[position_token_address] = {
                "factory_address": factory_address,
                "type": type_str,
                "abi_module": abi_module,
                "hook_addresses": hook_addresses,
                "state_view_address": state_view_address,
            }

    def _get_abi_module(self, type_str):
        abi_mapping = {
            "uniswapv4": uniswapv4_abi,
        }
        return abi_mapping.get(type_str)

    def get_position_by_factory(self, factory_address):
        entry = self.factory_to_position.get(factory_address)
        return entry.get("position_token_address") if entry else None

    def get_factory_by_position(self, position_token_address):
        entry = self.position_to_factory.get(position_token_address)
        return entry.get("factory_address") if entry else None
        
    def get_factory_by_hook(self, hook_address):
        return self.hook_to_factory.get(hook_address)

    def get_hooks_by_factory(self, factory_address):
        entry = self.factory_to_position.get(factory_address)
        return entry.get("hook_addresses", []) if entry else []

    def get_abi_by_factory(self, factory_address):
        entry = self.factory_to_position.get(factory_address)
        return entry["abi_module"] if entry else None

    def get_abi_by_position(self, position_token_address):
        entry = self.position_to_factory.get(position_token_address)
        return entry["abi_module"] if entry else None

    def get_type_str_by_position(self, position_token_address):
        entry = self.position_to_factory.get(position_token_address)
        return entry.get("type") if entry else None
        
    def get_state_view_address(self, factory_address):
        """
        Get the StateView contract address for a factory.
        The StateView contract provides utility functions to query pool state.
        """
        entry = self.factory_to_position.get(factory_address)
        return entry.get("state_view_address") if entry else None 