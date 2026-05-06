from fastapi import FastAPI, HTTPException, Depends
from neo4j import GraphDatabase
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
from database.driver import lifespan
import routers.nft as nft
import routers.transaction as transaction
import routers.collection as collection
import routers.wallet as wallet

app = FastAPI(lifespan = lifespan)

origins = [
    "http://localhost:3000",
    "http://localhost:10010",
    "http://localhost:9011"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(nft.router, prefix = '/api/nft', tags = ['NFT'])
app.include_router(transaction.router, prefix='/api/transaction', tags = ['Transaction'])
app.include_router(collection.router, prefix='/api/collection', tags=['Collection'])
app.include_router(wallet.router, prefix='/api/wallet', tags=['Wallet'])

# @app.get("/collections", response_model = CollectionList)
# def get_collections(db: GraphDatabase = Depends(get_db)):
#     return CollectionList(collections = ['Collection1','Figurine Panini','Pokemon','Cover','Collection A','Collection B'])

# @app.get("/nft-transactions/", response_model=NFTTransactionList)
# def get_nft_transactions(db: GraphDatabase = Depends(get_db)):
    # now = datetime.now()
    # transactions = [
    #     NFTTransaction(
    #         seller=1, 
    #         buyer=2, 
    #         transaction_id="id1", 
    #         nft_id="nft1", 
    #         collection="Collection A", 
    #         price=100,
    #         date=now - timedelta(days=5)
    #     ),
    #     NFTTransaction(
    #         seller=1, 
    #         buyer=3, 
    #         transaction_id="id2", 
    #         nft_id="nft2", 
    #         collection="Collection B", 
    #         price=150,
    #         date=now - timedelta(days=3)
    #     ),
    #     NFTTransaction(
    #         seller=3, 
    #         buyer=4, 
    #         transaction_id="id3", 
    #         nft_id="nft3", 
    #         collection="Collection A", 
    #         price=200,
    #         date=now - timedelta(days=1)
    #     )
    # ]
    # return NFTTransactionList(graph=transactions)