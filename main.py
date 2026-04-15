from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.routes import item_routes, product_routes, purchase_routes, stock_routes, issue_routes, return_routes, site_routes

app = FastAPI(title="Inventory API", description="API for managing inventory", version="1.0", root_path="/inventory")

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(item_routes.router)
app.include_router(product_routes.router)
app.include_router(purchase_routes.router)
app.include_router(stock_routes.router)
app.include_router(issue_routes.router)
app.include_router(return_routes.router)
app.include_router(site_routes.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1235)