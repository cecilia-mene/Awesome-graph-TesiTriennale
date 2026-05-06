from fastapi import APIRouter, HTTPException, Query, Request
from datetime import datetime

from neo4j import AsyncManagedTransaction
from schemas.response import (
    CollectionList,
    NFTTransaction,
    NFTTransactionList
)
from database.operations import get_collections as _get_collections
from database.operations import collection_networks as _collection_networks
from database.operations import nft_network as _nft_network
from database.operations import ntf_transaction_ts as _nft_transaction_ts
from database.operations import nft_summary as _nft_summary
from schemas.options import SamplingOption

router = APIRouter()

@router.get(
    '/collections',
    response_model = CollectionList,
    response_description = 'Return the list of the NFT collections'
)
async def get_collections(
    request: Request
) -> CollectionList:
    driver = request.app.graph_driver
    async with driver.session(database="neo4j") as session:
        result = await session.execute_read(_get_collections)
    if not result:
        raise HTTPException(
            status_code = 404,
            detail = 'An error occured while getting the collections'
        )
    return result

@router.get(
    '/collections_network',
    response_description = 'Transaction network of the NFTs in a collection'
)
async def get_collections_network(
    request: Request,
    collection: str = Query(description = 'Name of the collection'),
    limit: int = Query(description = 'Maximum number of networks for a collection', default = 5)
) -> list:
    driver = request.app.graph_driver
    async with driver.session(database="neo4j") as session:
        result = await session.execute_read(_collection_networks, collection, limit)
    if not result:
        raise HTTPException(
            status_code = 404,
            detail = 'An error occured while getting the collections'
        )
    return result

@router.get(
    '/nft_network',
    response_description = 'Transaction network for a specific NFT'
)
async def get_nft_network(
    request: Request,
    nft: str = Query(description = 'ID of the NFT')
) -> list:
    driver = request.app.graph_driver
    async with driver.session(database='neo4j') as session:
        results = await session.execute_read(_nft_network, nft)
    if not results:
        raise HTTPException(
            status_code = 404,
            detail = 'An error occured while getting the NFT'
        )
    return results

@router.get(
    '/transactions_ts',
    response_description = 'Time series of the transaction events for a NFT'
)
async def get_transactions_ts(
    request: Request,
    nft_id: str = Query(description = 'Identfier of a NFT in the format <nft>::<collection>'),
    start_date: datetime = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: datetime = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(descripttion = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver
    async with driver.session(database='neo4j') as session:
        results = await session.execute_read(_nft_transaction_ts, nft_id, start_date, end_date, sampling_frequency.value)
    if len(results) == 0:
        raise HTTPException(
            status_code = 404,
            detail = 'An error occured while getting the NFT'
        )
    return results

@router.get(
    '/summary',
    response_description = 'Summary of a NFT'
)
async def get_summary(
    request: Request,
    nft: str = Query(description = 'ID of the NFT(token_key)')
):
    driver = request.app.graph_driver
    async with driver.session(database = 'neo4j') as session:
        results = await session.execute_read(_nft_summary, nft)
    return results