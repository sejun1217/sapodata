import json
from pyrfc import Connection
import pprint
import boto3


ASHOST='' #Adjust
CLIENT='' #Adjust
SYSNR=''  #Adjust
USER=''   #Adjust
PASSWD ='' #Adjust

# RFC & Table settiong
RFC_Function = "RFC_PING"


# --------------------------
# Call RFC Function
# --------------------------

def _call_rfc_function():
    conn = Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD)

    print("----Begin of RFC---")
    result = conn.call(RFC_Function)
    
    # Print Result
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(result)
    
   
# ------------------------
# Start of Program
# ------------------------  

_call_rfc_function()
