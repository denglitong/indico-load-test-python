## Installation

    pip3 install -r requirements.txt

## Configuration

Visit https://YOUR_INDICO_HOST/auth/account and download your App Token to `~/indico_api_token.txt`. Your account should
have manager access to the workflow.

Configure the `HOST`, `INDICO_WORKFLOW_ID` to real value in config.py.

## Run Load Testing

```
locust --headless \
--users 20 \
--spawn-rate 10 \
--run-time 1min \
--csv=output \
--logfile output.log \
--stop-timeout 30
```

## sample load testing result

![summary.png](results/summary.png)