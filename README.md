# luigi
Demo project to get familiar with Luigi -> https://luigi.readthedocs.io/en/stable/index.html

# What it does?
![Flow](https://i.imgur.com/nN36Zbx.png)
![Luigi visualizer](https://i.imgur.com/eW8Rz1J.png)

# Prerequisites
python modules:
- luigi   
- zipfile 
- boto3   
- botocore
- sqlalchemy
- pandas

running mysql with employees DB

![Database](https://github.com/datacharmer/test_db/blob/master/images/employees.jpg?raw=true)

~/boto3 file config with aws crednetials

# How to run?
```$ python3 -m venv luigi-venv```

```$ . luigi-venv/bin/activate```

```$ python3 -m luigi --module hello-world MasterTask --local-scheduler```

