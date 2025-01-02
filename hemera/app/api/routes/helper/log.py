#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/31 13:54
# @Author  ideal93
# @File  log.py
# @Brief
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel
from sqlmodel import Session, desc, select

from hemera.app.core.service import extra_contract_service
from hemera.app.utils import ColumnType
from hemera.common.models.logs import Logs
from hemera.common.utils.abi_code_utils import decode_log_data
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes


class LogDetails(BaseModel):
    # Transaction related fields
    transaction_hash: str

    # Log specific fields
    log_index: int
    address: str
    data: str
    topic0: str
    topic1: Optional[str] = None
    topic2: Optional[str] = None
    topic3: Optional[str] = None

    # Block related fields
    block_number: int
    block_hash: str
    block_timestamp: datetime

    @staticmethod
    def from_db_model(log: Logs) -> "LogDetails":
        return LogDetails(
            transaction_hash=bytes_to_hex_str(log.transaction_hash),
            log_index=log.log_index,
            address=bytes_to_hex_str(log.address),
            data=bytes_to_hex_str(log.data),
            topic0=bytes_to_hex_str(log.topic0),
            topic1=bytes_to_hex_str(log.topic1) if log.topic1 else None,
            topic2=bytes_to_hex_str(log.topic2) if log.topic2 else None,
            topic3=bytes_to_hex_str(log.topic3) if log.topic3 else None,
            block_number=log.block_number,
            block_hash=bytes_to_hex_str(log.block_hash),
            block_timestamp=log.block_timestamp,
        )


class DecodedInputData(BaseModel):
    """Decoded input data for a log parameter"""

    indexed: bool
    name: str = ""
    data_type: str
    hex_data: str
    dec_data: str


class ContractInfo(BaseModel):
    address_display_name: Optional[str] = None
    function_name: Optional[str] = None
    full_function_name: Optional[str] = None
    function_unsigned: Optional[str] = None
    input_data: List[DecodedInputData] = []


class LogItem(LogDetails, ContractInfo):
    def __init__(self, log_details: LogDetails, contract_info: ContractInfo):
        """
        Initialize LogItem by combining LogDetails and ContractInfo.

        Args:
            log_details (LogDetails): The LogDetails instance.
            contract_info (ContractInfo): The ContractInfo instance.
        """
        combined_data = {**log_details.dict(), **contract_info.dict()}
        super().__init__(**combined_data)


def _process_log_columns(columns: ColumnType):
    """Process columns for Logs table"""
    if columns == "*":
        return select(Logs)

    if isinstance(columns, str):
        columns = columns.split(",")

    columns = [col.strip() for col in columns]
    return select(*[getattr(Logs, col) for col in columns])


def _get_logs_by_hash(session: Session, hash: str, columns: ColumnType = "*") -> List[Logs]:
    """Get logs by transaction hash

    Args:
        session: SQLModel session
        hash: Transaction hash (hex string) or bytes
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns from logs
                - "address,data": select specific columns
                - ["address", "data"]: select specific columns

    Returns:
        List[Logs]: List of logs matching the transaction hash
        When specific columns are selected, only those columns will be available

    Raises:
        ValueError: If hash format is invalid
    """
    if isinstance(hash, str):
        hash = hex_str_to_bytes(hash.lower())

    statement = _process_log_columns(columns)
    statement = statement.where(Logs.transaction_hash == hash)

    return session.exec(statement).all()


def _get_logs_by_address(
    session: Session, address: Union[str, bytes], columns: ColumnType = "*", limit: int = 25, offset: int = 0
) -> List[Logs]:
    """Get logs by contract address

    Args:
        session: SQLModel session
        address: Contract address (hex string) or bytes
        columns: Can be "*" for all columns, single column name, or list of column names
                Examples:
                - "*": select all columns from logs
                - "address,data": select specific columns
                - ["address", "data"]: select specific columns
        limit: Max number of logs to return
        offset: Number of logs to skip

    Returns:
        List[Logs]: List of logs matching the contract address
        When specific columns are selected, only those columns will be available

    Raises:
        ValueError: If address format is invalid
    """
    if isinstance(address, str):
        address = hex_str_to_bytes(address.lower())

    statement = _process_log_columns(columns)
    statement = statement.where(Logs.address == address).order_by(desc(Logs.block_number), desc(Logs.log_index))

    statement = statement.limit(limit)
    statement = statement.offset(offset)

    return session.exec(statement).all()


def get_logs_by_hash(session: Session, hash: str) -> List[LogDetails]:
    """Get logs by transaction hash

    Args:
        session: SQLModel session
        hash: Transaction hash (hex string)

    Returns:
        List[LogDetails]: List of logs matching the transaction hash
        When specific columns are selected, only those columns will be available
    """
    logs = _get_logs_by_hash(session, hash, "*")
    return [LogDetails.from_db_model(log) for log in logs]


def get_logs_by_address(session: Session, address: str, limit: int = 25, offset: int = 0) -> List[LogDetails]:
    """Get logs by contract address

    Args:
        session: SQLModel session
        address: Contract address (hex string)
        limit: Max number of logs to return
        offset: Number of logs to skip
    Returns:
        List[LogDetails]: List of logs matching the contract address
        When specific columns are selected, only those columns will be available
    """
    logs = _get_logs_by_address(session, address, "*", limit, offset)
    return [LogDetails.from_db_model(log) for log in logs]


def fill_extra_contract_info_to_logs(session: Session, log_list: List[LogDetails] = None) -> List[LogItem]:
    """Fill address display names to log entries

    Args:
        session: SQLModel session
        log_list: List of log dictionaries to enrich
    Returns:
        List[LogItem]: List of logs with contract info
    """
    contract_topic_list = []
    count_non_none = lambda x: 0 if x is None else 1
    for log in log_list:
        indexed_true_count = sum(count_non_none(topic) for topic in [log["topic1"], log["topic2"], log["topic3"]])
        contract_topic_list.append((log["address"], log["topic0"], indexed_true_count))
    # Get method list by transaction_method_list
    if extra_contract_service:
        address_sign_contract_abi_dict = extra_contract_service.get_abis_for_logs(contract_topic_list)
    else:
        address_sign_contract_abi_dict = {}
    for log_json in log_list:
        # Continue loop if 'topic0' is missing or has a falsy/empty value
        if not log_json.get("topic0"):
            continue
        # Set method id
        topic0_value = log_json["topic0"]
        log_json["method_id"] = topic0_value[0:10]

        event_abi = address_sign_contract_abi_dict.get((log_json["address"], topic0_value))
        if not event_abi:
            continue
        try:
            event_abi_json = json.loads(event_abi.get("function_abi"))
            # Get full data types
            index_data_types = []
            data_types = []

            # Get full data string
            index_data_str = ""
            data_str = log_json["data"][2:]

            for param in event_abi_json["inputs"]:
                if param["indexed"]:
                    index_data_types.append(param["type"])
                    index_data_str += log_json[f"topic{len(index_data_types)}"][2:]
                else:
                    data_types.append(param["type"])
            decoded_index_data, endcoded_index_data = decode_log_data(index_data_types, index_data_str)
            decoded_data, endcoded_data = decode_log_data(data_types, data_str)

            index_input_data = []
            input_data = []
            full_function_name = ""
            for index in range(len(event_abi_json["inputs"])):
                param = event_abi_json["inputs"][index]
                if param["indexed"]:
                    index_input_data.append(
                        {
                            "indexed": param["indexed"],
                            "name": param["name"],
                            "data_type": param["type"],
                            "hex_data": decoded_index_data[len(index_input_data)],
                            "dec_data": endcoded_index_data[len(index_input_data)],
                        }
                    )
                else:
                    input_data.append(
                        {
                            "indexed": param["indexed"],
                            "name": param["name"],
                            "data_type": param["type"],
                            "hex_data": decoded_data[len(input_data)],
                            "dec_data": endcoded_data[len(input_data)],
                        }
                    )
                if param["indexed"]:
                    full_function_name += f"index_topic_{index + 1} {param['type']} {param['name']}, "
                else:
                    full_function_name += f"{param['type']} {param['name']}, "
            function_name = event_abi.get("function_name")
            full_function_name = f"{function_name}({full_function_name[:-2]})"
            log_json["input_data"] = index_input_data + input_data
            log_json["function_name"] = function_name
            log_json["function_unsigned"] = event_abi.get("function_unsigned")
            log_json["full_function_name"] = full_function_name
        except Exception as e:
            log_json["input_data"] = []
            log_json["function_name"] = ""
            log_json["function_unsigned"] = ""
            log_json["full_function_name"] = ""
            log_json["error"] = str(e)
    fill_address_display_to_logs(session, log_list)
