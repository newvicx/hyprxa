from hyprxa.client import HyprxaClient



async def test_get():
    async with HyprxaClient("http://localhost:8000/") as client:
        doc = await client.get_topics()
        print(doc)


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_get())