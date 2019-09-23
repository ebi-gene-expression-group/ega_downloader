# Download EGA data and arrange for analysis

This is a Nextflow workflow designed to download data for an EGA dataset and arrange in a form suitable for analysis, with raw FASTQ files and a metadata table.

## Prerequisites

 * [Nextflow](https://www.nextflow.io/) installed
 * Aspera access to the dataset of interest. EGA will give you a download.sh script containing username and password, and a 'secret' for decrypting the files.

## Setup

### Directory structure

 * Create a directory for the dataset (with appropriate access restrictions - this is controlled access data)
 * Create 'data', 'metadata' and 'credentials' subdirectories

### Set up authentication

The 'download.sh' script EGA provides will download all the data for a dataset using Aspera, but to a fairly unpredictable subdirectory location. So we just extract the authentication information:

```
#!/bin/bash

#inits
export ASPERA_SCP_PASS="abcdef";
username="dbox1234"
destination="`dirname $0`"
parallel_downloads=8

#overwrite default parameters?
if [ ! -z $1 ]; then parallel_downloads="$1"; fi
if [ ! -z $2 ]; then destination="$2"; fi


#get small files in its most convenient way
ascp --ignore-host-key -E "*.aes" -E "*.cip" -E "*.crypt" -E "download.sh" -d -QTl 100m ${username}@xfer.crg.eu: ${destination}/

#get not small files in its most convenient way
cat ${destination}/dbox_content | xargs -i --max-procs=$parallel_downloads bash -c "mkdir -p $destination/\`dirname {}\`; echo \"Downloading {}, please wait ...\"; ascp --ignore-host-key -k 1 --partial-file-suffix=PART -QTl 100m ${username}@xfer.crg.eu:{} ${destination}/{} >/dev/null 2>&1"
``` 

You'll also separately be given a 'secret'.

Create an authentication file for each component dataset, at e.g. credentials/EGAD00011223344:

```
user=dbox1234
password=abcdef
secret=;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj
```

Make sure this file is readable only by yourself!

### Obtain metadata

Download the metadata bundle from the EGA page for each dataset. You'll get a bundle containing 'delimited_maps' and 'xmls'. Place these directories under 'metadata'.

EGA contains two central entity types: 'run' and 'analysis', containing raw (ish) and downstream analysis results, respectively. Unfortunately, EGA provides you with mappings of sample to file with no concept of whether they pertain to runs or analyses. You need a mapping from sample/run and sample/analysis pairs to file, which you'll need to obtain from the EGA. You should get a file with content like:

```
EGAN00009876543,EGAR00001234,[""abc.cram.gpg""]
EGAN00002198765,EGAR00005678,[""def.cram.gpg""]
EGAN00004321987,EGAR00009101,[""ghi.cram.gpg""]
```

Store this in a file called 'EGAN-EGAR.csv' under 'linkages'. Your metadata directory should now have subdirectories like:

```
metadata
    |- EGAD00011223344
        |- delimited_maps
        |- xmls
        |- linkages
```

We now have all the information we need to download and structure the raw data.

## Run download pipeline

Clone this repository to the top directory, then run:

```
./ega_tools/main.nf -resume
```

The result will be:

 * A metadata summary at a location like metadata/EGAD00011223344/ EGAD00011223344.merged.csv
 * Encrypted downloads at e.g. data/EGAD00011223344/encrypted
 * Decrypted files at data/(library strategy)/EGAD00011223344
 * FASTQ-converted files at data/EGAD00011223344/(library strategy)/fastq

If there are random failures you can resume by repeating the above command.

## Clean up

Nexflow leaves a few things lying around, so once the above has succeeded, remove them:

```
rm -rf .nextflow* work
```

