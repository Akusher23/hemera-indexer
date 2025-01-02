#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time  2024/12/19 16:24
# @Author  will
# @File  main.py
# @Brief
from fastapi import FastAPI

from hemera.app.api.routes.explorer.base import router as base_router
from hemera.app.api.routes.explorer.block import router as block_router
from hemera.app.api.routes.explorer.transaction import router as transaction_router

app = FastAPI()
app.include_router(block_router)
app.include_router(base_router)
app.include_router(transaction_router)
