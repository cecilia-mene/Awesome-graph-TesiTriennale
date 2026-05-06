from fastapi import APIRouter, HTTPException, Request, Query
from neo4j import AsyncManagedTransaction
from datetime import datetime
from schemas.response import (
    CollectionList
)
from schemas.options import SamplingOption

from database.operations import wallet_transactions_ts as _wallet_transactions_ts
from database.operations import wallet_summary as _wallet_summary
from database.operations import wallet_purchases as _wallet_purchases
from database.operations import wallet_purchases_ts as _wallet_purchases_ts
from database.operations import wallet_sales as _wallet_sales
from database.operations import wallet_sales_ts as _wallet_sales_ts
from database.operations import wallet_own as _wallet_own
from database.operations import wallet_gain as _wallet_gain

router = APIRouter()

@router.get(
    '/transactions_ts',
    response_description = 'Return the transaction of a wallet'
)
async def get_transactions_ts(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(descripttion = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver
    async with driver.session(database="neo4j") as session:
        results = await session.execute_read(_wallet_transactions_ts, wallet_id, start_date, end_date, sampling_frequency.value)
    if len(results) == 0:
        raise HTTPException(
            status_code = 404,
            detail = 'no transaction found for wallet'
        )
    return results

@router.get(
    '/summary',
    response_description = 'Summary of a wallet'
)
async def get_summary(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet')
):
    driver = request.app.graph_driver
    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_summary, wallet_id)
    if len(results) == 0:
        raise HTTPException(
            status_code = 404,
            detail = 'An error occured while getting the wallet'
        )
    return results
    
@router.get(
    '/purchase',
    response_description = 'Purchase of a wallet'
)
async def get_purchase(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss')
):
    driver = request.app.graph_driver

    # Convertiamo le date in stringhe nel formato richiesto
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_purchases, wallet_id, start_date_str, end_date_str)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No purchases found for wallet {wallet_id} in the specified time range'
        )

    return {"wallet_id": wallet_id, "NFT acquistati": results}

@router.get(
    '/purchase_ts',
    response_description = 'Purchase of a wallet'
)
async def get_purchase_ts(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(description = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver

     # Convertiamo le date in stringhe nel formato richiesto
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_purchases_ts, wallet_id, start_date_str, end_date_str, sampling_frequency.value)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No purchases found for wallet {wallet_id} in the specified time range'
        )

    return {"wallet_id": wallet_id, "numero NFT acquistati": results}

@router.get(
    '/sales',
    response_description = 'Sales of a wallet'
)
async def get_sales(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss')
):
    driver = request.app.graph_driver

    # Convertiamo le date in stringhe nel formato richiesto
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_sales, wallet_id, start_date_str, end_date_str)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No sales found for wallet {wallet_id} in the specified time range'
        )

    return {"wallet_id": wallet_id, "NFT venduti": results}

@router.get(
    '/sales_ts',
    response_description = 'Sales of a wallet'
)
async def get_sales_ts(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(description = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver

    # Convertiamo le date in stringhe nel formato richiesto
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_sales_ts, wallet_id, start_date_str, end_date_str, sampling_frequency.value)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No sales found for wallet {wallet_id} in the specified time range'
        )

    return {"wallet_id": wallet_id, "numero NFT venduti": results}

@router.get(
    '/own',
    response_description = 'NFT owned by a wallet'
)
async def get_own(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
):
    driver = request.app.graph_driver

    # Convertiamo la data in stringa nel formato richiesto
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_own, wallet_id, end_date_str)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No NFTs owned by wallet {wallet_id} at the specified date'
        )

    return {"wallet_id": wallet_id, "NFT posseduti": results}

@router.get(
    '/gain',
    response_description = 'Gain of a wallet'
)
async def get_gain(
    request: Request,
    wallet_id: str = Query(description = 'ID of the wallet'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss')
):
    driver = request.app.graph_driver

    # Convertiamo le date in stringhe nel formato richiesto
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_wallet_gain, wallet_id, end_date_str)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f'No gain found for wallet {wallet_id} in specified time'
        )

    return {"wallet_id": wallet_id, "NFT - Prezzo d'acquisto - Prezzo di vendita - Guadagno": results}