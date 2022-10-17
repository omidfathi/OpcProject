import asyncio
import logging

from asyncio_mqtt import Client, MqttError, ProtocolVersion

logger = logging.getLogger(__name__)

async def test() -> None:
        try:
            logger.info("Connecting to MQTT")
            async with Client(hostname="192.168.1.51", port=1883, client_id="OPC_client") as client:
                logger.info("Connection to MQTT open")
                async with client.filtered_messages("TimeSync") as messages:
                    await client.subscribe("TimeSync")
                    async for message in messages:
                        logger.info(
                            "Message %s %s", message.topic, message.payload
                        )
                await asyncio.sleep(2)
        except MqttError as e:
            logger.error("Connection to MQTT closed: " + str(e))
        except Exception:
            logger.exception("Connection to MQTT closed")
        await asyncio.sleep(3)


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.SelectorEventLoop().run_until_complete(asyncio.wait([test()]))
    # asyncio.get_event_loop().SelectorEventLoop(asyncio.wait([test()]))



if __name__ == "__main__":
    main()