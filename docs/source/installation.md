# HowTo

## Installation

```bash
python3 venv weatherlink_venv
pip install weatherlink2pg
```

## Usage

Usage require some variable environment as mentionned in file `.env.sample`
for weatherlink api authentication and database connection.

### Full download

Required as first launch

```sh
source weatherlink_env/bin/activate
source <env file>
weatherlink2pg full
```

### Update

```sh
source weatherlink_env/bin/activate
source <env file>
weatherlink2pg update
```

### crontask

recommanded way to update data using cron tasks

Create a script with the following code

```bash
#!/bin/bash

SCRIPT_RELATIVE_DIR=$(dirname "${BASH_SOURCE[0]}")

source $SCRIPT_RELATIVE_DIR/bin/activate

set -a # automatically export all variables
source  $SCRIPT_RELATIVE_DIR/.weatherlink2pg.env
set +a

weatherlink2pg update
```
