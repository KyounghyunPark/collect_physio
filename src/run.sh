#!/bin/bash

echo "collect-data.py"

requiredDate=${1}
echo 'python collect_data.py ' ${requiredDate}
python collect_data.py ${requiredDate}
