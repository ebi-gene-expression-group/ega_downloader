nextflow.enable.dsl=2

metadataDir = "${workflow.launchDir}/${params.metadataDir}"
egaCredentialsDir = "${workflow.launchDir}/${params.egaCredentialsDir}"

workflow {
    main:
        fetch_data(params.EGA_DATASET_ID)
        make_metadata_table(params.EGA_DATASET_ID)
}

process fetch_data {

    conda 'pyega3=5.2.0'

    cache 'lenient'

    input:
        val EGA_DATASET_ID

    output:
        file "ega_data/*"

    script:
        """
        mkdir -p ega_data
        # list unencrypted md5 checksums for all files
        pyega3 -cf $egaCredentialsDir/ega.credentials files $EGA_DATASET_ID
        # download the dataset
        pyega3 -c 10 -cf $egaCredentialsDir/ega.credentials fetch $EGA_DATASET_ID --output-dir ega_data --max-retries -1 --retry-wait 10
        """
}

process make_metadata_table {

    conda 'r-base r-xml2 r-optparse r-jsonlite r-dplyr'

    cache 'deep'

    input:
        val EGA_DATASET_ID

    output:
        file "ega_metadata/${EGA_DATASET_ID}.merged.csv"

    script:
        """
        mkdir -p ega_metadata
        ${workflow.launchDir}/bin/arrange_data.R -m $metadataDir -i $EGA_DATASET_ID -o ega_metadata/${EGA_DATASET_ID}.merged.csv
        """
}

