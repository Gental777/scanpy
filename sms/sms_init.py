from smsactivate.api import SMSActivateAPI

sa = SMSActivateAPI('c95474cd9A19be827e6e2296d7b9cfd8')
sa.debug_mode = True # Optional action. Required for debugging
result = sa.getBalance()
try:
    print(result['balance'])

except:
    print(result['message'])
