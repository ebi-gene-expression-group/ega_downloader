#!/usr/bin/env nextflow

dataDir = "${workflow.launchDir}/${params.dataDir}"
metadataDir = "${workflow.launchDir}/${params.metadataDir}"

DATASETS = Channel.fromPath( "$dataDir/EGAD*", type: 'dir' )

DATASETS.map{ dir -> tuple( file(dir).getName(), dir ) }
    .set {KEYED_DATASETS}

process make_metadata_table {

    conda 'r-base'

    publishDir "$metadataDir/$dsId", mode: 'copy', overwrite: true

    input:
        set val(dsId), file(dsPath) from KEYED_DATASETS

    output:
        file "${dsId}.merged.csv" into DATASET_CSVS

    """
    arrange_data.R $metadataDir $dataDir $dsId ${dsId}.merged.csv 
    """
}


DATASET_CSVS
    .splitCsv( header:true, sep:"\t" )
    .into{
        CSVS_FOR_CRAM
    }

CSVS_FOR_CRAM
    .filter{ row -> file(row['file_path']).getName().endsWith('.cram') }
    .map{ row -> tuple(row['ega_study_id'], row['biosample_id'], row['ega_run_id'], file(row['file_path'])) }
    .take( 1 )
    .set{
        CRAMS_FOR_FASTQS
    }

process cram_to_fastq {

    conda 'samtools'

    input:
        set val(egaStudyId), val(biosampleId), val(egaRunId), file(cramFile) from CRAMS_FOR_FASTQS

    output:
        set val(egaStudyId), val(biosampleId), val(egaRunId), file("${egaRunId}_1.fastq.gz"), file("${egaRunId}_2.fastq.gz") into FASTQS_FROM_CRAMS
    
    """
    export REF_CACHE=$dataDir/reference
    samtools collate $cramFile ${cramFile}.collated
    samtools fastq -F 2816 -c 6 -1 ${egaRunId}_1.fastq.gz -2 ${egaRunId}_2.fastq.gz ${cramFile}.collated
    """
}
