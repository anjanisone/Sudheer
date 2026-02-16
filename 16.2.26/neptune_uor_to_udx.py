from gremlin_python.driver import client, serializer

NEPTUNE_ENDPOINT = "wss://your-neptune-endpoint:8182/gremlin"
OLD_PREFIX = "uor_"
NEW_PREFIX = "udx_"

gremlin_client = client.Client(
    NEPTUNE_ENDPOINT,
    'g',
    message_serializer=serializer.GraphSONSerializersV2d0()
)

query = f"""
g.V().
  has('source_name', startingWith('{OLD_PREFIX}')).
  sideEffect{{
      it.get().property(
          'source_name',
          it.get().value('source_name').replace('{OLD_PREFIX}','{NEW_PREFIX}')
      )
  }}.count()
"""

result = gremlin_client.submit(query).all().result()
print(result)
gremlin_client.close()
