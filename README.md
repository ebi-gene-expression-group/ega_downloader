# Download EGA data and arrange for analysis

This is a Nextflow workflow designed to download data for an EGA dataset and arrange in a form suitable for analysis, with raw FASTQ files and a metadata table. 

The  workflow uses the [pyega3](https://github.com/EGA-archive/ega-download-client) client to pull files directly from EGA.


## Prerequisites

 * [Nextflow](https://www.nextflow.io/) installed. Current version tested with nextflow 24.04.3
 * Formal access to the datasets of interest in [EGA](https://ega-archive.org/)
 * SLURM cluster management and job scheduling system
 * An account and access the Seqera Platform (optional)

## Setup

### Directory structure

 * Create a directory for the dataset (with appropriate access restrictions - this is controlled access data)
 * Create 'metadata' and 'credentials' subdirectories

### Set up authentication

#### Python client method

The python client authenticates via your user account, place a file called 'ega.credentials' in the credentials folder. It will look like:

```
{
        "username": "me@foo.bar.uk",
        "password": "abc123",
}
``` 

See the [EGA documentation](https://ega-archive.org/access/download/files/pyega3) for more info.


### Obtain metadata

Download the metadata bundle from the EGA page for each dataset. You'll have the option to download a zipped file with metadata in TSV, CSV and JSON. Download the CSV version, unzip it and place the files under 'metadata':

```
metadata
    |- analyses.csv
    |- analysis_sample.csv
    |- ...
```

We now have all the information we need to download the raw data and process the metadata.

## Run download pipeline

Clone this repository to the top directory. 

Then run:

```
source envs.sh
nextflow run main.nf -c nextflow.config --EGA_DATASET_ID $EGA_DATASET_ID
```

... or


```
nextflow run main.nf -c nextflow.config --EGA_DATASET_ID $EGA_DATASET_ID -with-tower
```

to leverage the [Seqera Platform](https://docs.seqera.io/platform/24.1.1/getting-started/deployment-options) capabilities. You'll need to obtain a token and add it to `envs.sh`.

The result will be:

 * A metadata summary at a location like work/..../ega_metadata/EGAD00011223344.merged.csv
 * FASTQ files at data/.../ega_data/(EGAFxxxxx)/fastq


## Clean up

Nexflow leaves a few things lying around, so once the above has succeeded, remove them:

```
rm -rf .nextflow*
```

## Legacy code
A previous implementation was used that required BAM/ CRAM files downloaded from EGA to be converted to fastq in an endedness-specific manner (i.e. paired endedness detected and handled correctly). The previous workflow, in addition of using the pyega3 client to pull files directly from EGA, included also the Aspera dropbox method to download files from a 'dropbox' provided to you from EGA staff - this method is now deprecated. See release v1.0.0 for more information on the earlier version.

