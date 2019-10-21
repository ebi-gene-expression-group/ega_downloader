#!/usr/bin/env Rscript 

suppressPackageStartupMessages(library('xml2'))

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

# Generate a run/file mapping from the XMLs

runs <- list.files(file.path(metadata_dir, ds, 'xmls', 'runs'), full.names = TRUE)

run_xml_content <- lapply(runs, function(x){
  run <- read_xml(x)
  run_info <- attributes(as_list(run)$RUN_SET$RUN$DATA_BLOCK$FILES$FILE)
  run_info$primary_id <- unlist(as_list(run)$RUN_SET$RUN$IDENTIFIERS$PRIMARY_ID)
  run_info$ega_run_id <- basename(sub('.run.xml', '', x))
  unlist(run_info)
})

common_fields <- Reduce(intersect, lapply(run_xml_content, names))
run_file <- data.frame(do.call(rbind, lapply(run_xml_content, function(x) x[common_fields])), stringsAsFactors = FALSE)
run_file$filename <- gsub('/', '_', run_file$filename)

sample_info <- merge(sample_info, run_file, all.x = TRUE, sort = FALSE)
sample_info$filename <- sub('(.*)\\.gpg', '\\1', sample_info$filename)
sample_info$filename <- sub('(.*)\\.cip', '\\1', sample_info$filename)

# Now check the file is actually available for download

dbox_files <- readLines(dbox_listing)

# Assume that the dbox entry contains the file name and the run ID

matches <- apply(sample_info, 1, function(x) which(grepl(x['ega_run_id'], dbox_files) & grepl(x['filename'], dbox_files)))
names(matches) <- sample_info$filename
lengths <- unlist(lapply(matches, length))

if (any(lengths != 1)){
    missing <- names(lengths)[lengths == 0]
    if (length(missing) > 0){
        write(paste("can't find the following in the downloads:", paste(missing, collapse=', ')), stderr())
    }
    multiple <- names(lengths)[lengths > 1]
    if (length(multiple) > 0){
        write(paste("The following match multiple files in downloads:", paste(multiple, collapse=', ')), stderr())
    }

    q(status=1)
}

sample_info$dbox_path = dbox_files[unlist(matches)]
sample_info$file <- sub('.crypt', '', basename(sample_info$dbox_path))

# Stash the dataset ID in the file for convenience

sample_info$ega_dataset_id <- ds

write.table(sample_info, outfile, quote = FALSE, row.names=FALSE, sep="\t")
