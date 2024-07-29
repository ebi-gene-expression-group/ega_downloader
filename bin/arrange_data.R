#!/usr/bin/env Rscript 

# arrange_data.R -m $metadataDir -i $dsId  -o ${dsId}.merged.csv 

suppressPackageStartupMessages(library('xml2'))
suppressPackageStartupMessages(library('optparse'))
suppressPackageStartupMessages(library('jsonlite'))
suppressPackageStartupMessages(library('dplyr'))


# parse options

cl <- commandArgs(trailingOnly = TRUE)

option_list = list(
  make_option(
    c("-m", "--metadata-dir"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Path to a top-level directory where CSV formatted metadata are stored"
  ),
  make_option(
    c("-i", "--dataset-id"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Dataset ID"
  ),
  make_option(
    c("-o", "--output-file"),
      action = "store",
      default = NA,
      type = 'character',
      help = "Output filename"
    )
)

opt <- parse_args(OptionParser(option_list = option_list), convert_hyphens_to_underscores = TRUE)


for (required in c('metadata_dir', 'dataset_id', 'output_file')){
    if (is.na(opt[[required]])){
        stop(paste0('ERROR: No ', required, ' specified'))
    }
} 

# check if EGA metadata files exist
file_names <- c(
    'sample_file.csv',
    'dataset.csv',
    'samples.csv',
    'study_experiment_run_sample.csv',
    'experiments.csv',
    'runs.csv',
    'studies.csv'
)

files_to_check <- file.path(opt$metadata_dir, file_names)

file_existence <- sapply(files_to_check, file.exists)

if (any(!file_existence)) {
    missing_files <- files_to_check[!file_existence]
    error_message <- paste("ERROR: The following files do not exist:\n", paste(missing_files, collapse = "\n"))
    stop(error_message)
}

cat("All EGA metadata files exist.\n")


# 1 create metadata table and update with information from sample_file.csv
sample_file <- read.csv( paste0(opt$metadata_dir,'/sample_file.csv') )

metadata <- as.data.frame(matrix(NA, nrow=nrow(sample_file), ncol= 29) )

colnames(metadata) <- c(
    "ega_sample_id",             
    "sample_alias",              
    "ega_run_id",                
    "ega_study_id",              
    "study_title",               
    "study_type",                
    "instrument_platform",       
    "instrument_model",          
    "library_layout",            
    "library_name",              
    "library_strategy",          
    "library_source",            
    "library_selection",         
    "ega_experiment_id",         
    "filename",                  
    "filetype",                  
    "gender",                    
    "phenotype",                 
    "ega_dataset_id",            
    "file_accession_id",         
    "submission_accession_id",   
    "created_at",                
    "edited_at",                 
    "biosample_id",              
    "library_construction_protocol",
    "paired_nominal_length",     
    "paired_nominal_sdev",       
    "design_description",        
    "total_num_samples"          
)


metadata <- metadata %>%
    mutate(
        ega_sample_id = sample_file$sample_accession_id,
        filename = sample_file$file_name,
        sample_alias = sample_file$sample_alias,
        file_accession_id = sample_file$file_accession_id
    )


# 2 update metadata with information from dataset.csv
dataset <- read.csv( paste0(opt$metadata_dir,'/dataset.csv') )

metadata <- metadata %>%
    mutate(
        ega_dataset_id = dataset$accession_id,
        submission_accession_id = dataset$submission_accession_id,
        created_at = dataset$created_at,
        edited_at = dataset$edited_at,
        total_num_samples = dataset$num_samples
    )


# 3 update metadata with information from samples.csv
samples <- read.csv( paste0(opt$metadata_dir,'/samples.csv') )

for (i in 1:length(samples$accession_id) ) {
    i_m <- which(metadata$ega_sample_id == samples$accession_id[i] )

    if ("phenotype" %in% names(samples)) {
        metadata$phenotype[i_m] <- samples$phenotype[i]
    }
    if ("biological_sex" %in% names(samples)) {
        metadata$gender[i_m] <- samples$biological_sex[i]
    }        
        
    # add extra attributes
    if (i == 1) {
        # parse the JSON string
        extra_attributes <- fromJSON( samples$extra_attributes[i] )
        # removes all parentheses and replace spaces with underscores in tag
        extra_attributes$tag <- gsub("[()]", "", gsub(" ", "_", extra_attributes$tag))
        
        unique_tags <- unique(extra_attributes$tag)
        
        # add columns to metadata if they don't exist
        for (tag in unique_tags) {
            
            if (!tag %in% names(metadata)) {
                metadata[[tag]] <- NA
            }
        }
        
        # add .unit columns for tags with non-NA, non-empty unit
        for (i in 1:nrow(extra_attributes)) {
            tag <- extra_attributes$tag[i]
            unit <- extra_attributes$unit[i]
            if (!is.na(unit) && unit != "") {
                unit_col <- paste0(tag, ".unit")
                if (!unit_col %in% names(metadata)) {
                    metadata[[unit_col]] <- NA
                }
            }
        }
        # add values
        for (j in 1:length( extra_attributes$tag ) ){
            if ( extra_attributes$tag[j] %in% names(metadata) ) {
                metadata[i_m, extra_attributes$tag[j] ] <- extra_attributes$value[j]
            }
        }
    
    } else {
        extra_attributes <- fromJSON( samples$extra_attributes[i] )
        extra_attributes$tag <- gsub("[()]", "", gsub(" ", "_", extra_attributes$tag))
        # add values
        for (j in 1:length( extra_attributes$tag ) ){
            if ( extra_attributes$tag[j] %in% names(metadata) ) {
                metadata[i_m, extra_attributes$tag[j] ] <- extra_attributes$value[j]
            }
        }
    }
}


# 4 update metadata with information from study_experiment_run_sample.csv
study_experiment_run_sample <-  read.csv( paste0(opt$metadata_dir,'/study_experiment_run_sample.csv') )

matched_indices <- match(metadata$sample_alias, study_experiment_run_sample$sample_alias)

metadata$ega_run_id <- study_experiment_run_sample$run_accession_id[matched_indices]
metadata$ega_experiment_id <- study_experiment_run_sample$experiment_accession_id[matched_indices]

columns_matched_indices <- c("biosample_id", 
             "library_strategy", "library_source", "library_layout", 
             "library_name", "library_selection", "study_title", 
             "study_type", "instrument_platform", "instrument_model")

for (col in columns_matched_indices) {
    metadata[[col]] <- study_experiment_run_sample[[col]][matched_indices]
}


# 5 update metadata with information from experiments.csv
experiments <- read.csv( paste0(opt$metadata_dir,'/experiments.csv') )

matched_indices <- match(metadata$ega_experiment_id, experiments$accession_id)

metadata$ega_study_id <- experiments$study_accession_id[matched_indices]
metadata$library_construction_protocol <- experiments$library_construction_protocol[matched_indices]
metadata$paired_nominal_length <- experiments$paired_nominal_length[matched_indices]
metadata$paired_nominal_sdev <- experiments$paired_nominal_sdev[matched_indices]
metadata$design_description <- experiments$design_description[matched_indices]

# 6 update metadata with information from runs.csv
runs <- read.csv( paste0(opt$metadata_dir,'/runs.csv') )

matched_indices <- match(metadata$ega_run_id, runs$accession_id)

metadata$filetype <- runs$run_file_type[matched_indices]

# 7 update metadata with information from studies.csv
studies <- read.csv( paste0(opt$metadata_dir,'/studies.csv') )

if (nrow(studies) == 1){
    extra_attributes_study <- fromJSON( studies$extra_attributes )
    extra_attributes_study$tag <- gsub("[()]", "", gsub(" ", "_", extra_attributes_study$tag))
    
    if (!extra_attributes_study$tag %in% names(metadata)) {
        metadata[[extra_attributes_study$tag]] <- NA
    }
    metadata[[extra_attributes_study$tag]] <- extra_attributes_study$value 
}


# save file in the form EGADXXXXXX.merged.csv
write.table(metadata, opt$output_file, quote = FALSE, row.names=FALSE, sep="\t")
saveRDS(metadata, paste0(opt$dataset_id, '.rds') )
