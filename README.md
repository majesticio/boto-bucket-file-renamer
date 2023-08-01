# Rename all archive files in an AWS bucket
------
### First:
`pip install boto3`

## Update renamer.py
*add your credentials*
```python
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
SOURCE_BUCKET=
DEST_BUCKET=
DELETE_BUCKET=
REGION_NAME=
WORKERS=
LOG_LEVEL=
```
*choose the correct bucket(!)*
```python
## Use the right bucket!
bucket_name = "paste-the-correct-bucket-name-here"
rename_files(bucket_name)
```
## Use it
*this will take some time!*

`python renamer.py`

*or*

`python3 renamer.py`