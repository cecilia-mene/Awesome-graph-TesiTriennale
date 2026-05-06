from neo4j import AsyncManagedTransaction
from neo4j.time import DateTime
from schemas.response import CollectionList, NFTTransactionList
import networkx as nx
from datetime import datetime
from pandas import DatetimeIndex
import pandas as pd
import numpy as np

async def get_collections(
    tx: AsyncManagedTransaction
) -> CollectionList:
    query = '''
        MATCH (n:COLLECTION)
        RETURN n.collection AS name
    '''
    response = await tx.run(query)
    result = await response.data()
    if result:
        return CollectionList(collections = [r['name'] for r in result])
    return CollectionList(collections = [])

async def collection_networks(
    tx: AsyncManagedTransaction,
    collection: str,
    limit: int
) -> list:
    query = '''
        match (c:COLLECTION {collection:$collection})<-[:IN_COLL]-(nft:NFT)<-[:TRANS_FOR_NFT]-(t:TRANSACTION)<-[:BUY]-(b:WALLET)
        match (s:WALLET)-[:SELL]->(t)
        return s.wallet_id as u_of_edge, b.wallet_id as v_of_edge, 
            t.transaction_hash as transaction_id, nft.token_key as nft_id, t.price_usd as price, 
            t.transaction_date as date, c.collection as collection_name
    '''
    collection_network = nx.DiGraph()
    response = await tx.run(query, collection = collection)
    results = await response.data()
    if results:
        for r in results:
            collection_network.add_edge(**r)
        con_comps = [collection_network.subgraph(cset).copy() for cset in ([c for c in sorted(nx.weakly_connected_components(collection_network), key=len, reverse=True)][:limit])]
        response = []
        for cc in con_comps:
            response.append([f'{s},{t},{props["transaction_id"]},{props["nft_id"]},{props["collection_name"]},{props["price"]},{props["date"]}' for s,t,props in cc.edges(data=True)])
        return response
    return []

async def nfts_in_collection(
    tx: AsyncManagedTransaction,
    collection: str
) -> list:
    query = '''
        MATCH (c:COLLECTION {collection:$collection})<-[:IN_COLL]-(nft:NFT)
        RETURN nft.token_key as nft_id
    '''
    response = await tx.run(query, collection = collection)
    results = await response.data()
    if results:
        return [r['nft_id'] for r in results]
    return []

async def collection_transaction_ts(
        tx: AsyncManagedTransaction,
        collection: str,
        start_date: str,
        end_date: str,
        sampling_frequency: str
):
    query = '''
        match (c:COLLECTION {collection:$collection})<-[:IN_COLL]-(nft:NFT)<-[:TRANS_FOR_NFT]-(t:TRANSACTION)
        where t.transaction_date >= datetime($start_date) and t.transaction_date <= datetime($end_date)
        return t.transaction_date as ts 
    '''
    response = await tx.run(query, collection = collection, start_date = start_date, end_date = end_date)
    results = await response.data()
    print(len(results))
    if results:
        return pd.Series(np.ones(len(results)), index = [r['ts'].to_native() for r in results]).resample(sampling_frequency).sum()
    return []

async def collection_summary(
        tx: AsyncManagedTransaction,
        collection: str
):
    query = '''
        MATCH (c:COLLECTION {collection:$collection})<-[:IN_COLL]-(nft:NFT)<-[:TRANS_FOR_NFT]-(t:TRANSACTION)
        RETURN 
            c.collection AS collection_name,
            COUNT(DISTINCT nft) AS total_nfts,
            COUNT(DISTINCT t) AS total_transactions,
            MIN(t.transaction_date) AS first_transaction,
            MAX(t.transaction_date) AS last_transaction
    '''
    query2 = '''
        MATCH (c:COLLECTION {collection:$collection})<-[:IN_COLL]-(nft:NFT)<-[:TRANS_FOR_NFT]-(t:TRANSACTION)
        WITH nft, c, t
        ORDER BY t.transaction_date DESC
        WITH nft, COLLECT(t)[0] AS latest_transaction 
        RETURN 
            SUM(latest_transaction.price_usd) AS total_price
    '''
    response = await tx.run(query, collection = collection)
    response_price = await tx.run(query2, collection = collection)
    results = await response.data()
    results_price = await response_price.data()
    response_dict = {'name': '', 'total_nfts' : 0, 'total_transaction': 0, 'value': 0}
    if results:
        info, price = results[0], results_price[0]['total_price']
        response_dict['name'] = info['collection_name']
        response_dict['total_nfts'] = info['total_nfts']
        response_dict['total_transaction'] = info['total_transactions']
        response_dict['min_date'] = info['first_transaction'].to_native()
        response_dict['max_date'] = info['last_transaction'].to_native()
        response_dict['value'] = price
    return response_dict

async def nft_network(
        tx: AsyncManagedTransaction,
        nft: str
) -> list:
    query = '''
        match (nft:NFT {token_key:$nft})<-[:TRANS_FOR_NFT]-(t:TRANSACTION)<-[:BUY]-(b:WALLET)
        match (nft)-[:IN_COLL]->(c:COLLECTION)
        match (s:WALLET)-[:SELL]->(t)
        return  t.transaction_hash as transaction_id, nft.token_key as nft_id, t.price_usd as price, 
            t.transaction_date as date, s.wallet_id as sell, b.wallet_id as buy, c.collection as collection_name
    '''
    response = await tx.run(query, nft = nft)
    results = await response.data()
    if results:
        return_response = [f'{r["sell"]},{r["buy"]},{r["transaction_id"]},{r["nft_id"]},{r["collection_name"]},{r["price"]},{r["date"]}' for r in results]
        return return_response
    return []

async def transaction_ts(
        tx: AsyncManagedTransaction,
        start_date: str,
        end_date: str,
        sampling_frequency: str
):
    query = '''
        match (t:TRANSACTION)
        where t.transaction_date >= datetime($start_date) and t.transaction_date <= datetime($end_date)
        return t.transaction_date as ts 
    '''
    response = await tx.run(query, start_date = start_date, end_date = end_date)
    results = await response.data()
    if results:
        return pd.Series(np.ones(len(results)), index = [r['ts'].to_native() for r in results]).resample(sampling_frequency).sum()
    return []

async def ntf_transaction_ts(
        tx: AsyncManagedTransaction,
        nft_id: str,
        start_date: str,
        end_date: str,
        sampling_frequency: str
):
    query = '''
        match (nft:NFT {token_key:$nft_id})<-[:TRANS_FOR_NFT]-(t:TRANSACTION)
        where t.transaction_date >= datetime($start_date) and t.transaction_date <= datetime($end_date)
        return t.transaction_date as ts 
    '''
    response = await tx.run(query, start_date = start_date, end_date = end_date, nft_id = nft_id)
    results = await response.data()
    print(results)
    if results:
        return pd.Series(np.ones(len(results)), index = [r['ts'].to_native() for r in results]).resample(sampling_frequency).sum()
    return []

async def nft_summary(
        tx: AsyncManagedTransaction,
        nft: str
): 
    query = '''
        match (nft:NFT {token_key:$nft})<-[:TRANS_FOR_NFT]-(t:TRANSACTION)
        with
        nft,
        MIN(t.transaction_date) AS first_transaction_date,
        MAX(t.transaction_date) AS last_transaction_date,
        COUNT(DISTINCT t) AS total_transactions

        match (nft)<-[:TRANS_FOR_NFT]-(last_t:TRANSACTION)<-[:BUY]-(last_w:WALLET)
        where last_t.transaction_date = last_transaction_date

        match (nft)<-[:TRANS_FOR_NFT]-(first_t:TRANSACTION)<-[:SELL]-(creator_w:WALLET)
        where first_t.transaction_date = first_transaction_date
        
        RETURN 
            nft,
            first_transaction_date,
            last_transaction_date,
            total_transactions,
            last_w.wallet_id as last_buyer,
            creator_w.wallet_id as creator_wallet
        '''
    response = await tx.run(query, nft = nft)
    results = await response.data()
    response_dict = {'id': '', 'first_transaction': '', 'last_transaction': '', 'creator_wallet': '', 'last_buyer': '', 'number_of_transaction': 0}
    if results:
        info = results[0]
        response_dict['id'] = nft
        response_dict['first_transaction'] = info['first_transaction_date'].to_native()
        response_dict['last_transaction'] = info['last_transaction_date'].to_native()
        response_dict['number_of_transaction'] = info['total_transactions']
        response_dict['creator_wallet'] = info['creator_wallet']
        response_dict['last_buyer'] = info['last_buyer']
    return response_dict

async def wallet_transactions_ts(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        start_date: str,
        end_date: str,
        sampling_frequency: str
):
    query = '''
        match (WALLET {wallet_id:$wallet_id})-[BUY]->(t:TRANSACTION)
        return t.transaction_date as data, t.price_usd AS prezzo
    '''
    response = await tx.run(query, start_date = start_date, end_date = end_date, wallet_id = wallet_id)
    results = await response.data()
    print(results)
    if results:
        # Creare un DataFrame con le date e i prezzi
        df = pd.DataFrame([{'data': r['data'].to_native(), 'prezzo': r['prezzo']} for r in results])
        df.set_index('data', inplace=True)  # Usare la data come indice
        # Eseguire il resampling per sommare i prezzi
        resampled = df['prezzo'].resample(sampling_frequency).sum()
        return resampled

    return []

async def wallet_purchases(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        start_date: str,
        end_date: str
) -> list:
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[b:BUY]->(t:TRANSACTION)-[TRANS_FOR_NFT]->(nft:NFT)
        WHERE t.transaction_date >= datetime($start_date) AND t.transaction_date <= datetime($end_date)
        RETURN nft.token_key AS token_key
        '''
    response = await tx.run(query, wallet_id = wallet_id, start_date = start_date, end_date = end_date)
    results = await response.data()
    if results:
        return [r['token_key'] for r in results]
    
    #Restituisce la serie temporale del numero di NFT acquistati da wallet_id nel periodo temporal compreso tra start_date e end_date, utilizzando come frequenza di campionamento l'opzione assegnata a sampling

async def wallet_purchases_ts(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        start_date: str,
        end_date: str,
        sampling_frequency: str
) -> list:
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[b:BUY]->(t:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
        WHERE t.transaction_date >= datetime($start_date) AND t.transaction_date <= datetime($end_date)
        WITH datetime({year: t.transaction_date.year, month: t.transaction_date.month, day: t.transaction_date.day}) AS date_grouped, COUNT(DISTINCT nft) AS nft_count
        RETURN date_grouped AS date, nft_count
        ORDER BY date
    '''
    response = await tx.run(query, wallet_id=wallet_id, start_date=start_date, end_date=end_date)
    results = await response.data()
    print(results)
    if results:
        df = pd.DataFrame([{'date': r['date'].to_native(), 'nft_count': r['nft_count']} for r in results])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index)  # Converte esplicitamente in datetime
        
        # Resampling secondo la frequenza di campionamento
        resampled = df.resample(sampling_frequency).sum().reset_index()
        
        return resampled.to_dict(orient='records')  # Converte in lista di dizionari
    
    return []

    #Restituisce la lista degli NFT venduti da wallet_id nel periodo temporal compreso tra start_date e end_date

async def wallet_sales(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        start_date: str,
        end_date: str
) -> list:

    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[s:SELL]->(t:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
        WHERE t.transaction_date >= datetime($start_date) AND t.transaction_date <= datetime($end_date)
        RETURN nft.token_key AS token_key
        '''
    response = await tx.run(query, wallet_id = wallet_id, start_date = start_date, end_date = end_date)
    results = await response.data()
    if results:
        return [r['token_key'] for r in results]
    return []

# Restituisce la serie temporale del numero di NFT venduti da wallet_id nel periodo temporal compreso tra start_date e end_date, utilizzando come frequenza di campionamento l'opzione assegnata a sampling
async def wallet_sales_ts(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        start_date: str,
        end_date: str,
        sampling_frequency: str
) -> list:
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[s:SELL]->(t:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
        WHERE t.transaction_date >= datetime($start_date) AND t.transaction_date <= datetime($end_date)
        WITH datetime({year: t.transaction_date.year, month: t.transaction_date.month, day: t.transaction_date.day}) AS date_grouped, COUNT(DISTINCT nft) AS nft_count
        RETURN date_grouped AS date, nft_count
        ORDER BY date
        '''
    response = await tx.run(query, wallet_id = wallet_id, start_date = start_date, end_date = end_date)
    results = await response.data()
    print(results)
    if results:
        df = pd.DataFrame([{'date': r['date'].to_native(), 'nft_count': r['nft_count']} for r in results])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index)  # Converte esplicitamente in datetime
        
        # Resampling secondo la frequenza di campionamento
        resampled = df.resample(sampling_frequency).sum().reset_index()
        
        return resampled.to_dict(orient='records')  # Converte in lista di dizionario
    return []

async def wallet_summary(
        tx: AsyncManagedTransaction,
        wallet_id: str
):
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[b:BUY]->(t_buy:TRANSACTION)
WITH 
    w,
    COUNT(DISTINCT t_buy) AS total_buy_transactions,
    SUM(t_buy.price_usd) AS total_buy,
    MIN(t_buy.transaction_date) AS first_buy_date

MATCH (w)-[s:SELL]->(t_sell:TRANSACTION)
WITH 
    w,
    total_buy_transactions,
    total_buy,
    first_buy_date,
    COUNT(DISTINCT t_sell) AS total_sell_transactions,
    SUM(t_sell.price_usd) AS total_sell,
    MAX(t_sell.transaction_date) AS last_sell_date

// NFT attualmente posseduti (acquistati ma non rivenduti)
MATCH (w)-[:BUY]->(t:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
WHERE NOT EXISTS {
    MATCH (nft)<-[:TRANS_FOR_NFT]-(:TRANSACTION)<-[:SELL]-(w)
}

// Date della prima e ultima transazione (acquisti e vendite)
WITH 
    total_buy_transactions,
    total_buy,
    total_sell_transactions,
    total_sell,
    COLLECT(nft.token_key) AS nft_ids,
    first_buy_date,
    last_sell_date

RETURN 
    total_buy_transactions,
    total_buy,
    total_sell_transactions,
    total_sell,
    nft_ids,
    first_buy_date,
    last_sell_date

    '''

    response = await tx.run(query, wallet_id = wallet_id)
    results = await response.data()
    response_dict = {'wallet_id': '', 'total_buy_transactions': 0, 'total_buy': 0, 'total_sell_transactions': 0, 'total_sell': 0, 'nft_ids': [], 'first_buy_date': '', 'last_sell_date': ''}
    if results:
        info = results[0]
        response_dict['wallet_id'] = wallet_id
        response_dict['total_buy_transactions'] = info['total_buy_transactions']
        response_dict['total_buy'] = info['total_buy']
        response_dict['total_sell_transactions'] = info['total_sell_transactions']
        response_dict['total_sell'] = info['total_sell']
        response_dict['nft_ids'] = info['nft_ids']
        response_dict['first_buy_date'] = info['first_buy_date'].to_native()
        response_dict['last_sell_date'] = info['last_sell_date'].to_native()
    return response_dict

async def wallet_own(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        end_date: str
) -> list:
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[b:BUY]->(t:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
        WHERE t.transaction_date <= datetime($end_date)
        AND NOT EXISTS {
            MATCH (nft)<-[:TRANS_FOR_NFT]-(t2:TRANSACTION)<-[:SELL]-(w)
            WHERE t2.transaction_date <= datetime($end_date)
        }
        RETURN nft.token_key AS token_key
        '''
    response = await tx.run(query, wallet_id = wallet_id, end_date = end_date)
    results = await response.data()
    if results:
        return [r['token_key'] for r in results]
    return []

async def wallet_gain(
        tx: AsyncManagedTransaction,
        wallet_id: str,
        end_date: str
) -> list:
    query = '''
        MATCH (w:WALLET {wallet_id:$wallet_id})-[b:BUY]->(t_buy:TRANSACTION)-[:TRANS_FOR_NFT]->(nft:NFT)
        WHERE t_buy.transaction_date <= datetime($end_date)

        MATCH (nft)<-[:TRANS_FOR_NFT]-(t_sell:TRANSACTION)<-[:SELL]-(w)
        WHERE t_sell.transaction_date <= datetime($end_date)

        RETURN 
        nft.token_key AS token_key,
        t_buy.price_usd AS buy_price, 
        t_sell.price_usd AS sell_price,
        (t_sell.price_usd - t_buy.price_usd) AS gain
        '''
    
    response = await tx.run(query, wallet_id = wallet_id, end_date = end_date)
    results = await response.data()
    if results:
        return [f'{r["token_key"]},{r["buy_price"]},{r["sell_price"]},{r["gain"]}' for r in results]
    return []
