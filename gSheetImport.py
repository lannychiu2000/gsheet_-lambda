GSHEET_NAME_ATTRIBUTE = 'name'
GSHEET_ID_ATTRIBUTE = 'id'
GSHEET_MODIFIED_TIME_ATTRIBUTE = 'modifiedTime'

def handler(event, context):
  for record in event['Records']:
    messageAttributes=record["messageAttributes"]
    gSheetID = messageAttributes[GSHEET_ID_ATTRIBUTE].get('stringValue')
    gSheetName = messageAttributes[GSHEET_NAME_ATTRIBUTE].get('stringValue')
    gSheetModifiedTime = messageAttributes[GSHEET_MODIFIED_TIME_ATTRIBUTE].get('stringValue')
    
    print ("Starting import of gSheet {} (ID: {})".format(gSheetName, gSheetID))
    # Add import functionality here

if __name__ == '__main__':
  handler(event,'')