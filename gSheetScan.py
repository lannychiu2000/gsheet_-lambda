import boto3
from boto3.dynamodb.conditions import Key
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Google API
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'service.json'
GOOGLE_DRIVE_ID = '0ADq4ronJcL3eUk9PVA'
GOOGLE_DRIVE_FOLDER_ID = '1RqfxFoL_ybMj17qOxZIvWjmQFVywKSkM'
GSHEET_MIME_TYPE = 'application/vnd.google-apps.spreadsheet'
GSHEET_NAME_ATTRIBUTE = 'name'
GSHEET_ID_ATTRIBUTE = 'id'
GSHEET_MODIFIED_TIME_ATTRIBUTE = 'modifiedTime'

# AWS - Dynamo
DYNAMODB_TABLE = 'gSheetsModified'
DYNAMODB = boto3.resource('dynamodb')

# AWS - SQS
SQS = boto3.resource('sqs')
SQS_QUEUE_NAME = 'gSheetImportQueue'

def dynamo_add_gsheet_record(gSheet):
  table = DYNAMODB.Table(DYNAMODB_TABLE)
  response = table.put_item(
     Item=gSheet
  )
  return response

def dynamo_find_gsheet_record(gSheetID):
    table = DYNAMODB.Table(DYNAMODB_TABLE)
    response = table.query(
        KeyConditionExpression=Key('id').eq(gSheetID)
    )
    try:
      return response['Items'][0]
    except IndexError:
      # No match found, return None
      return None

def send_import_event(gSheet):
  gSheetID = gSheet.get(GSHEET_ID_ATTRIBUTE)
  gSheetName = gSheet.get(GSHEET_NAME_ATTRIBUTE)
  gSheetModifiedTime = gSheet.get(GSHEET_MODIFIED_TIME_ATTRIBUTE)
  message = "Sending import event to SQS queue for gSheet " + gSheetID
  print (message)
  queue = SQS.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
  response = queue.send_message(
    MessageAttributes={
        GSHEET_ID_ATTRIBUTE: {
            'DataType': 'String',
            'StringValue': gSheetID
        },
        GSHEET_NAME_ATTRIBUTE: {
            'DataType': 'String',
            'StringValue': gSheetName
        },
        GSHEET_MODIFIED_TIME_ATTRIBUTE: {
            'DataType': 'String',
            'StringValue': gSheetModifiedTime
        }
    },
    MessageBody=message,
  )
  return

def handler(event, context):
  # Authenticate to Google
  creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
  service = build('drive', 'v3', credentials=creds, cache_discovery=False)
  page_token = None

  while True:
    # Search for gSheets on Drive API
    response = service.files().list(
      q = "'" + GOOGLE_DRIVE_FOLDER_ID + "' in parents and mimeType = '" + GSHEET_MIME_TYPE + "'",
      driveId = GOOGLE_DRIVE_ID,
      includeItemsFromAllDrives = True,
      supportsAllDrives = True,
      corpora = 'drive',
      fields='nextPageToken, files(id, name, modifiedTime)',
      pageToken=page_token).execute()

    for gSheet in response.get('files', []):
      gSheetID = gSheet.get('id')
      gSheetName = gSheet.get('name')
      gSheetModifiedTime = gSheet.get('modifiedTime')
      print ("Found Google Drive file: {} ({})".format(gSheetName, gSheetModifiedTime))

      print ("Querying Dynamo DB to find previous last modified time")
      dynamodbResponse = dynamo_find_gsheet_record(gSheetID)

      if dynamodbResponse is None:
        # No matching record suggests first run for gSheet
        print ("No previous timestamp found. Adding timestamp to DB for gSheet {} (ID: {})".format(gSheetName, gSheetID))
        putGSheetResponse = dynamo_add_gsheet_record(gSheet)

        print ("Running import for gSheet {} (ID: {})".format(gSheetName, gSheetID))
        send_import_event(gSheet)
      else:
        if dynamodbResponse.get(GSHEET_MODIFIED_TIME_ATTRIBUTE, '') == gSheetModifiedTime:
          print ("Previous timestamp matches current timestamp, Skipping import for gSheet {} (ID: {})".format(gSheetName, gSheetID))
        else:
          print ("Previous timestamp differs from current timestamp, Running import for gSheet {} (ID: {})".format(gSheetName, gSheetID))
          send_import_event(gSheet)
      print ("")

    page_token = response.get('nextPageToken', None)
    if page_token is None:
      break

if __name__ == '__main__':
    handler('','')