#!/usr/bin/env Rscript 

suppressPackageStartupMessages(library('xml2'))
suppressPackageStartupMessages(library('optparse'))

# Parse options

cl <- commandArgs(trailingOnly = TRUE)

option_list = list(
  make_option(
    c("-m", "--metadata-dir"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Path to a top-level directory where metadata are stored"
  ),
  make_option(
    c("-d", "--data-dir"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Path to a top-level directory where data are stored"
  ),
  make_option(
    c("-i", "--dataset-id"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Dataset ID- subdirectories named like this should be found in data and metadata directories"
  ),
  make_option(
    c("-x", "--dbox-listing"),
      action = "store",
      default = NA,
      type = 'character',
      help = "dbox listing (when aspera being used)- to check files indicated in metadata are present"
  ),
  make_option(
    c("-o", "--output-file"),
      action = "store",
      default = NA,
      type = 'character',
      help = "dbox listing (when aspera being used)- to check files indicated in metadata are present"
    )
)

opt <- parse_args(OptionParser(option_list = option_list), convert_hyphens_to_underscores = TRUE)

for (required in c('metadata_dir', 'dataset_id', 'output_file')){
    if (is.na(opt[[required]])){
      die(paste0('ERROR: No', required, 'specified'))
    }
} 

# Read in the file that will allow us to derive a list of runs (i.e not analyses)

print(file.path(opt$metadata_dir, opt$dataset_id, 'delimited_maps', "Study_Experiment_Run_sample.map"))

sample_info <- read.delim(file.path(opt$metadata_dir, opt$dataset_id, 'delimited_maps', "Study_Experiment_Run_sample.map"), header=FALSE, stringsAsFactors = FALSE)
colnames(sample_info) <- c('ega_study_id', 'study_title', 'study_type', 'instrument_platform', 'instrument_model', 'library_layout', 'maybe_read_count', 'library_strategy', 'library_source', 'molecule', 'ega_experiment_id', 'ega_run_id', 'centre', 'unknown', 'ega_sample_id')
sample_info$file_key <- paste(sample_info$ega_sample_id, sample_info$ega_run_id, sep='-')

if (max(table(sample_info$file_key)) > 1){
    write("Non-unique sample/ run pairs in Study_Experiment_Run_sample.map", stderr())
    q(status=1)
}

# Generate a run/file mapping from the XMLs

runs <- list.files(file.path(opt$metadata_dir, opt$dataset_id, 'xmls', 'runs'), full.names = TRUE)

run_xml_content <- lapply(runs, function(x){
  run <- read_xml(x)
  run_info <- data.frame(do.call(rbind, lapply(as_list(read_xml(x))$RUN_SET$RUN$DATA_BLOCK$FILES, function(y) unlist(attributes(y)))), stringsAsFactors=F)
  run_info$primary_id <- unlist(as_list(run)$RUN_SET$RUN$IDENTIFIERS$PRIMARY_ID)
  run_info$ega_run_id <- basename(sub('.run.xml', '', x))
  run_info
})

common_fields <- Reduce(intersect, lapply(run_xml_content, names))
run_file <- data.frame(do.call(rbind, lapply(run_xml_content, function(x) x[, common_fields])), stringsAsFactors = FALSE)
run_file$filename <- gsub('/', '_', run_file$filename)

sample_info <- merge(sample_info, run_file, all.x = TRUE, sort = FALSE)
sample_info$filename <- sub('(.*)\\.gpg', '\\1', sample_info$filename)
sample_info$filename <- sub('(.*)\\.cip', '\\1', sample_info$filename)

# Derive sample metadata from sample XMLs

sample_files <- list.files(file.path(opt$metadata_dir, opt$dataset_id, 'xmls', 'samples'), full.names = TRUE)

sample_xml_content <- lapply(sample_files, function(sample_file){
  sample <- read_xml(sample_file)
  fields <- lapply(as_list(sample)$SAMPLE_SET$SAMPLE$SAMPLE_ATTRIBUTES, function(x) unlist(x[['TAG']]))
  values <- lapply(as_list(sample)$SAMPLE_SET$SAMPLE$SAMPLE_ATTRIBUTES, function(x) unlist(x[['VALUE']]))
  sample_info <- structure(values, names = fields)
  sample_info$ena_sample_id <- unlist(as_list(sample)$SAMPLE_SET$SAMPLE$IDENTIFIERS$PRIMARY_ID)
  sample_info$ega_sample_id <- basename(sub('.sample.xml', '', sample_file))
  
  unlist(sample_info)
})

common_fields <- Reduce(intersect, lapply(sample_xml_content, names))
sample_xml_info <- data.frame(do.call(rbind, lapply(sample_xml_content, function(x) x[common_fields])), stringsAsFactors = FALSE)
sample_info <- merge(sample_info, sample_xml_info, by = 'ega_sample_id')

if ( ! is.na(opt$dbox_listing)){

    # Now check the file is actually available for download

    dbox_files <- readLines(opt$dbox_listing)

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
}

# Stash the dataset ID in the file for convenience

sample_info$ega_dataset_id <- opt$dataset_id

saveRDS(sample_info, "foo.rds")
write.table(sample_info, opt$output_file, quote = FALSE, row.names=FALSE, sep="\t")
