import os
import ssl

from tablestore import *
from example_config import *

# The following code shows how to set the TLS version used by OTSClient.

access_key_id = OTS_ACCESS_KEY_ID
access_key_secret = OTS_ACCESS_KEY_SECRET

# Create a connection with TLS version 1.2
ots_client = OTSClient(OTS_ENDPOINT, access_key_id, access_key_secret, OTS_INSTANCE,
                       region=OTS_REGION, ssl_version=ssl.PROTOCOL_TLSv1_2)

# do something
resp = ots_client.list_table()
print(resp)
