from fastapi import APIRouter, HTTPException, Request, Query
from neo4j import AsyncManagedTransaction
from datetime import datetime

from database.operations import nfts_in_collection  as _nfts_in_collection
from database.operations import collection_transaction_ts as _collection_transaction_ts
from database.operations import collection_summary as _collection_summary

from schemas.options import SamplingOption

router = APIRouter()

@router.get(
    '/nfts',
    response_description = 'List of the timestamp of transactions in a specifi time window'
)
async def nfts_in_collection(
    request: Request,
    collection: str = Query(descripttion = 'Name of the collection')
):
    driver = request.app.graph_driver
    async with driver.session(database="neo4j") as session:
        result = await session.execute_read(_nfts_in_collection, collection)
        if len(result) == 0:
            raise HTTPException(
                status_code = 404,
                detail = f'An error occured while getting the NFTs in the collection {collection}'
            )
        return result
    
@router.get(
    '/transaction_ts',
    response_description = 'List of the timestamp of transaction in a specific time window'
)
async def get_transactions_ts(
    request: Request,
    collection: str = Query(description = 'Name of the collection'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(descripttion = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver
    async with driver.session(database='neo4j') as session:
        results = await session.execute_read(_collection_transaction_ts, collection, start_date, end_date, sampling_frequency.value)
    if len(results) == 0:
        raise HTTPException(
            status_code = 404,
            detail = f'An error occured while getting the timestamps of the collection: {collection}'
        )
    return results

@router.get(
    '/summary',
    response_description = 'Summary of a collection'
)
async def get_summary(
    request: Request,
    collection: str = Query(description = 'Name of the collection')
):
    driver = request.app.graph_driver
    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_collection_summary, collection)
    return results