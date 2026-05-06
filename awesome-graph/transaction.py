from fastapi import APIRouter, HTTPException, Request, Query
from neo4j import AsyncManagedTransaction
from datetime import datetime

from database.operations import transaction_ts as _transaction_ts
from schemas.options import SamplingOption

router = APIRouter()

@router.get(
    '/transaction_time',
    response_description = 'List of the timestamp of transactions in a specifi time window'
)
async def transaction_time(
    request: Request,
    start_date: str = Query(description = 'Initial date in format YYYY-MM-DD HH:mm:ss'),
    end_date: str = Query(description = 'Final date in format YYYY-MM-DD HH:mm:ss'),
    sampling_frequency: SamplingOption = Query(descripttion = 'Sampling frequency for building the time series', default = SamplingOption.daily)
):
    driver = request.app.graph_driver
    async with driver.session(database="neo4j") as session:
        result = await session.execute_read(_transaction_ts, start_date, end_date, sampling_frequency.value)
        print(result)
        if len(result) == 0:
            raise HTTPException(
                status_code = 404,
                detail = 'An error occured while getting the collections'
            )
        return result
