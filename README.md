# dataverse-batch-processing
Provides means to do batch processing actions on a Dataverse instance 
using its API's. 

The Dataverse research data repository system has a nice API that is described well 
in their [guides](https://guides.dataverse.org/en/latest/api/index.html).
Most examples use curl on the commandline, which works great. 
In my first attempts at batch processing I did use curl in a bash shell script 
that processed a list of PIDs from the standard input. 
Even though you can of course do everything in bash, 
I preferred using Python when things got a bit more complicated. 

There are two Python libraries for using the API. 
The [Dataverse API Client](https://github.com/IQSS/dataverse-client-python)
and [pyDataverse](https://pydataverse.readthedocs.io/en/latest/). 
These are not used, but it might be interesting to try using them at some point. 

The next useful source for code are stand-alone Python scripts 
that already support batch processing;
 [dataverse-scripts](https://github.com/jggautier/dataverse_scripts)
Here we see lots of code duplication which I want to avoid 
and have more of a framework or library for doing batch processing instead. 

 
Usage
-----
 
This project was setup with Poetry, a dependency management tool for Python: [Poetry docs](https://python-poetry.org/docs/). 
I imported it in PyCharm with the Poetry Plugin installed 
and I am running the code from the IDE, while editing it to my needs. 

To get it working you need to create a directory 'work' at `../work/config.ini` relative to the directory of the batch_processing.py file. 
This is important, because we want to avoid that you accidentally share secret information 
by 'committing' and 'pushing' to Github for example. 
Then copy the `config.ini.empty` file into it and rename by dropping the `.empty` at the end of the filename. 
Fill in the correct values for the API key and the Dataverse server URL. 
Also create a `pids_to_process.txt` file or whatever other name you have configured, 
and place the doi's in there with `doi:` prefix and every doi on it's own line. 
After a 'run' the directory tree will look similar to the following: 

```
├── README.md (this file)
├── batch_processing
│   ├── ... code
├── ... other stuff
└── work
    ├── config.ini
    ├── pids_mutated_20211101_174616.txt
    └── pids_to_process.txt
```

