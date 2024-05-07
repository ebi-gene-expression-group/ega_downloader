# Download EGA data and arrange for analysis

This is a Nextflow workflow designed to download data for an EGA dataset and arrange in a form suitable for analysis, with raw FASTQ files and a metadata table. BAM/ CRAM files are converted to fastq in an endedness-specific manner (i.e. paired endedness is detected and handled correctly).

There are two operating modes:

 * Use the [pyega3](https://github.com/EGA-archive/ega-download-client) client to pull files directly from EGA. This will only work for datasets without the older .gpg encryption, with all files using .cip.
 * Download files from a 'dropbox' provided to you from EGA staff. This is usually done where there are issues using the official client (this mode is currently deprecated).

Note that there is no provision to use the old EGA Java client, which we have found difficult to use successfully.

In both cases the idea is to make sure that the files available correspond to those stated in the dataset metadata, and to parallelise the download of those files.

## Prerequisites

 * [Nextflow](https://www.nextflow.io/) installed
 * Formal access to the datasets of interest in EGA
 * For the dropbox download method, Aspera access to the dataset of interest. EGA will give you a download.sh script containing username and password, and a 'secret' for decrypting the files.

## Setup

### Directory structure

 * Create a directory for the dataset (with appropriate access restrictions - this is controlled access data)
 * Create 'data', 'metadata' and 'credentials' subdirectories

### Set up authentication

#### Python client method

The python client authenticates via your user account, place a file called 'ega.credentials' in the credentials folder. It will look like:

```
{
        "username": "me@foo.bar.uk",
        "password": "abc123",
        "client_secret": "klj;lkj;lkajf;lkja;lkfjsa;lkfja;lkfja;lsfdkja;lfkja;slfkjas;flkja;flkjas;flkjas;lkj"
}
``` 

See the [EGA documentation](https://ega-archive.org/access/download/files/pyega3) for more info.

#### Dropbox method

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

Create an authentication file for each component dataset, at e.g. credentials/EGAD00011223344.txt:

```
user=dbox1234
password=abcdef
secret=;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj;lkj
```

Make sure this file is readable only by yourself!

### Obtain metadata

Download the metadata bundle from the EGA page for each dataset. You'll get a bundle containing 'delimited_maps' and 'xmls'. Place these directories under 'metadata':

```
metadata
    |- EGAD00011223344
        |- delimited_maps
        |- xmls
```

We now have all the information we need to download and structure the raw data.

## Run download pipeline

Clone this repository to the top directory. For Aspera dropbox, you'll need to copy EGA's 'decryptor.jar' app into the bin/ directory of this repo (since I didn't think it appropriate to redistribute that file).

Then run:

```
./ega_downloader/main.nf -resume -fetchMode=pyclient
```

... to use the python client to fetch files, or:


```
./ega_downloader/main.nf -resume -fetchMode=aspera
```

... to pull files from an Aspera dropbox.

The result will be:

 * A metadata summary at a location like metadata/EGAD00011223344/ EGAD00011223344.merged.csv
 * Encrypted downloads at e.g. data/EGAD00011223344/encrypted (dropbox method only)
 * Decrypted files at data/(library strategy)/EGAD00011223344
 * FASTQ-converted files at data/EGAD00011223344/(library strategy)/fastq

If there are random failures you can resume by repeating the above command.


## Clean up

Nexflow leaves a few things lying around, so once the above has succeeded, remove them:

```
rm -rf .nextflow* work
```

