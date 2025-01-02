from enum import Enum
from typing import Dict, List, Optional, Union

from sqlmodel import Session, select

from hemera.app.api.routes.helper import ColumnType
from hemera.app.core.service import extra_contract_service, extra_ens_service
from hemera.common.models.address import AddressIndexStats
from hemera.common.models.contracts import Contracts
from hemera.common.models.tokens import Tokens
from hemera.common.utils.format_utils import bytes_to_hex_str, hex_str_to_bytes

# Type definitions
