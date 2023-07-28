# Rename all archive files in an AWS bucket
------
### First:
`pip install boto3`

## Update renamer.py
*add your credentials*
```python
aws_access_key_id = "your-access-key-here"
aws_secret_access_key = "your-secret-key-here"
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