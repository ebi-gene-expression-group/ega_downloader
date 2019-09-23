#!/usr/bin/env Rscript 

# Find the datasets listed in the metadata directory

cl <- commandArgs( trailingOnly = TRUE )

metadata_dir <- cl[1]
data_dir <- cl[2]
ds <- cl[3]
dbox_listing <- cl[4]
outfile <- cl[5]

# Read in the file that will allow us to derive a list of runs (i.e not analyses)

print(file.path(metadata_dir, ds, 'delimited_maps', "Study_Experiment_Run_sample.map"))

sample_info <- read.delim(file.path(metadata_dir, ds, 'delimited_maps', "Study_Experiment_Run_sample.map"), header=FALSE, stringsAsFactors = FALSE)
colnames(sample_info) <- c('ega_study_id', 'study_title', 'study_type', 'instrument_platform', 'instrument_model', 'library_layout', 'maybe_read_count', 'library_strategy', 'library_source', 'molecule', 'ega_experiment_id', 'ega_run_id', 'centre', 'unknown', 'ega_sample_id')
sample_info$file_key <- paste(sample_info$ega_sample_id, sample_info$ega_run_id, sep='-')

if (max(table(sample_info$file_key)) > 1){
    write("Non-unique sample/ run pairs in Study_Experiment_Run_sample.map", stderr())
    q(status=1)
}

# Read in the list of files pertaining to each sample. These will be a
# mixture of files for runs and for analyses

sample_file_mapping <- read.delim(file.path(metadata_dir, ds, 'delimited_maps', "Sample_File.map"), header=FALSE, stringsAsFactors = FALSE)
colnames(sample_file_mapping) <- c('biosample_id', 'ega_sample_id', 'file', 'ega_file_id')
sample_file_mapping$file <- sub('(.*)\\.cip', '\\1', sample_file_mapping$file )

# If analyses are present concerning the same samples as the runs, then
# there is no way to differentiate files pertaining to runs and to analyses
# without talking to ENA. 

if (max(table(sample_file_mapping$ega_sample_id)) > 1){
    sample_run_file_file <- file.path(metadata_dir, ds, 'linkages', "EGAN-EGAR.csv")
    if (file.exists(sample_run_file_file)){
        sample_run_file <- read.delim(sample_run_file_file, header=FALSE, stringsAsFactors = FALSE, sep=',')
        colnames(sample_run_file) <- c('ega_sample_id', 'ega_run_id', 'file')
        sample_run_file$file_key <- paste(sample_run_file$ega_sample_id, sample_run_file$ega_run_id, sep='-')
        sample_run_file$file <- sub('\\[(.*)\\]', '\\1', sample_run_file$file)
        sample_run_file$file <- sub('(.*)\\.gpg', '\\1', sample_run_file$file)

        if (max(table(sample_run_file$file_key)) > 1){
            write(paste("Non-unique sample/ run pairs in", sample_run_file), stderr())
            q(status=1)
        }
    }else{
        write(paste("There are multiple files per sample in this EGA study, and no way of mapping them to runs or analyses with default EGA info. You need to contact helpdesk@ega-archive.org, and ask for a 3-column linkage file containing sample ID, run ID and file name(s). Place this at", sample_run_file_file, "."), stderr())    
        q(status=1)

    }

    # Get the name of the file from the mapping EGA provided

    sample_info$file <- sample_run_file$file[match(sample_info$file_key, sample_run_file$file_key)]
      
    # Use the standard EGA sample/ file mapping to get the EGA file ID for
    # the file. We'll need that to derive the path from the data download

    sample_info <- merge(sample_info, sample_file_mapping[,-2], by='file', sort=FALSE)
}else{
    sample_info <- merge(sample_info, sample_file_mapping, by='ega_sample_id', sort=FALSE)
}

# Now check the file is actually available for download

dbox_files <- readLines(dbox_listing)
matches <- lapply(sample_info$file, function(x) grep(x, dbox_files))
lengths <- unlist(lapply(matches, length))

if (any(lengths != 1)){
    write("Cannot match files to downloads", stderr())
    q(status=1)
}

sample_info$dbox_path = dbox_files[unlist(matches)]
sample_info$file <- sub('.crypt', '', basename(sample_info$dbox_path))

# Stash the dataset ID in the file for convenience

sample_info$ega_dataset_id <- ds
write.table(sample_info, outfile, quote = FALSE, row.names=FALSE, sep="\t")
