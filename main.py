from fastapi import FastAPI

# Create an instance of FastAPI
app = FastAPI()


# Define a route with a path "/"
@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

# Run the app using Uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8888)

