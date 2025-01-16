#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hemera.common.utils.abi_code_utils import Event, Function

OMEGA_BORROW_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "borrower",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "Borrow",
    "type": "event"
})

OMEGA_DEPOSIT_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "lender",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "Deposit",
    "type": "event"
})

OMEGA_REPAY_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "borrower",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "Repay",
    "type": "event"
})

OMEGA_WITHDRAW_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "lender",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "recipient",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "Withdraw",
    "type": "event"
})

OMEGA_ACCOUNT_BORROW_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "asset",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "AccountBorrowed",
    "type": "event"
})


OMEGA_ACCOUNT_CREATE_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address",
            "name": "account",
            "type": "address"
        }
    ],
    "name": "AccountCreated",
    "type": "event"
})

OMEGA_ACCOUNT_COLLATERAL_WITHDRAW_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address",
            "name": "receiver",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "CollateralWithdrawal",
    "type": "event"
})

OMEGA_ACCOUNT_COLLATERAL_DEPOSIT_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address",
            "name": "sender",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
        }
    ],
    "name": "CollateralDeposit",
    "type": "event"
})


OMEGA_ACCOUNT_ACCOUNT_REPAID_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "address[]",
            "name": "assets",
            "type": "address[]"
        },
        {
            "indexed": False,
            "internalType": "uint256[]",
            "name": "amounts",
            "type": "uint256[]"
        }
    ],
    "name": "AccountRepaid",
    "type": "event"
})

OMEGA_ACCOUNT_COLLATERAL_LIQUIDATION_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "debt",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "collateral",
            "type": "address"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "debtAmountNeeded",
            "type": "uint256"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "collateralAmount",
            "type": "uint256"
        },
        {
            "indexed": False,
            "internalType": "uint256",
            "name": "bonusCollateral",
            "type": "uint256"
        }
    ],
    "name": "CollateralLiquidation",
    "type": "event"
})

OMEGA_ACCOUNT_STRATEGY_ACTIVATE_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "strategy",
            "type": "address"
        }
    ],
    "name": "StrategyActivated",
    "type": "event"
})


OMEGA_ACCOUNT_STRATEGY_DEACTIVATE_EVENT = Event({
    "inputs": [
        {
            "indexed": True,
            "internalType": "address",
            "name": "owner",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "account",
            "type": "address"
        },
        {
            "indexed": True,
            "internalType": "address",
            "name": "strategy",
            "type": "address"
        }
    ],
    "name": "StrategyDeactivated",
    "type": "event"
})
   
   
    # {
    #     "inputs": [
    #         {
    #             "internalType": "address",
    #             "name": "account",
    #             "type": "address"
    #         }
    #     ],
    #     "name": "getAccountHealth",
    #     "outputs": [
    #         {
    #             "components": [
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "collateralValue",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "totalInvestmentValue",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "healthFactor",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "bool",
    #                     "name": "isLiquidatable",
    #                     "type": "bool"
    #                 },
    #                 {
    #                     "internalType": "bool",
    #                     "name": "isRisky",
    #                     "type": "bool"
    #                 },
    #                 {
    #                     "internalType": "bool",
    #                     "name": "hasBadDebt",
    #                     "type": "bool"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "totalDebtValue",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "totalBorrowLimit",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "borrowFactoredDebtValue",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "components": [
    #                         {
    #                             "internalType": "address",
    #                             "name": "asset",
    #                             "type": "address"
    #                         },
    #                         {
    #                             "internalType": "uint256",
    #                             "name": "amount",
    #                             "type": "uint256"
    #                         },
    #                         {
    #                             "internalType": "uint256",
    #                             "name": "value",
    #                             "type": "uint256"
    #                         },
    #                         {
    #                             "internalType": "uint256",
    #                             "name": "borrowFactoredValue",
    #                             "type": "uint256"
    #                         }
    #                     ],
    #                     "internalType": "struct AccountLib.Debt[]",
    #                     "name": "debts",
    #                     "type": "tuple[]"
    #                 }
    #             ],
    #             "internalType": "struct AccountLib.Health",
    #             "name": "health",
    #             "type": "tuple"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [
    #         {
    #             "internalType": "address",
    #             "name": "account",
    #             "type": "address"
    #         },
    #         {
    #             "internalType": "uint256",
    #             "name": "index",
    #             "type": "uint256"
    #         }
    #     ],
    #     "name": "getActiveStrategy",
    #     "outputs": [
    #         {
    #             "internalType": "address",
    #             "name": "",
    #             "type": "address"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # }
    
    


    # {
    #     "inputs": [
    #         {
    #             "indexed": False,
    #             "internalType": "UD60x18",
    #             "name": "index",
    #             "type": "uint256"
    #         }
    #     ],
    #     "name": "BorrowIndexUpdated",
    #     "type": "event"
    # }
    # {
    #     "inputs": [
    #         {
    #             "indexed": False,
    #             "internalType": "UD60x18",
    #             "name": "rate",
    #             "type": "uint256"
    #         }
    #     ],
    #     "name": "BorrowRateUpdated",
    #     "type": "event"
    # }
    # {
    #     "inputs": [
    #         {
    #             "indexed": False,
    #             "internalType": "UD60x18",
    #             "name": "index",
    #             "type": "uint256"
    #         }
    #     ],
    #     "name": "LiquidityIndexUpdated",
    #     "type": "event"
    # },
    # {
    #     "inputs": [
    #         {
    #             "indexed": False,
    #             "internalType": "UD60x18",
    #             "name": "rate",
    #             "type": "uint256"
    #         }
    #     ],
    #     "name": "LiquidityRateUpdated",
    #     "type": "event"
    # },
    
    # {
    #     "inputs": [],
    #     "name": "getBorrowRate",
    #     "outputs": [
    #         {
    #             "internalType": "UD60x18",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [
    #         {
    #             "internalType": "address",
    #             "name": "borrower",
    #             "type": "address"
    #         }
    #     ],
    #     "name": "getDebtAmount",
    #     "outputs": [
    #         {
    #             "internalType": "uint256",
    #             "name": "debt",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [
    #         {
    #             "internalType": "address",
    #             "name": "lender",
    #             "type": "address"
    #         }
    #     ],
    #     "name": "getDepositAmount",
    #     "outputs": [
    #         {
    #             "internalType": "uint256",
    #             "name": "balance",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getLiquidityRate",
    #     "outputs": [
    #         {
    #             "internalType": "UD60x18",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getNormalizedDebt",
    #     "outputs": [
    #         {
    #             "internalType": "UD60x18",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getNormalizedIncome",
    #     "outputs": [
    #         {
    #             "internalType": "UD60x18",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getReserve",
    #     "outputs": [
    #         {
    #             "components": [
    #                 {
    #                     "internalType": "contract IERC20",
    #                     "name": "asset",
    #                     "type": "address"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "assetBalance",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "borrowRate",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "liquidityRate",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "liquidityIndex",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "borrowIndex",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "lastUpdateTimestamp",
    #                     "type": "uint256"
    #                 }
    #             ],
    #             "internalType": "struct ILendingPool.Reserve",
    #             "name": "",
    #             "type": "tuple"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getSettings",
    #     "outputs": [
    #         {
    #             "components": [
    #                 {
    #                     "internalType": "contract IInterestRateStrategy",
    #                     "name": "strategy",
    #                     "type": "address"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "lendingFeeShare",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "UD60x18",
    #                     "name": "flashLoanFeeShare",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "depositCap",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "minimumFeeCollectionAmount",
    #                     "type": "uint256"
    #                 },
    #                 {
    #                     "internalType": "uint256",
    #                     "name": "minimumOpenBorrow",
    #                     "type": "uint256"
    #                 }
    #             ],
    #             "internalType": "struct ILendingPool.Settings",
    #             "name": "",
    #             "type": "tuple"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getTotalBorrow",
    #     "outputs": [
    #         {
    #             "internalType": "uint256",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # },
    # {
    #     "inputs": [],
    #     "name": "getTotalSupply",
    #     "outputs": [
    #         {
    #             "internalType": "uint256",
    #             "name": "",
    #             "type": "uint256"
    #         }
    #     ],
    #     "stateMutability": "view",
    #     "type": "function"
    # }
