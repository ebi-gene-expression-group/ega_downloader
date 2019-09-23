#!/usr/bin/env nextflow

dataDir = "${workflow.launchDir}/${params.dataDir}"
metadataDir = "${workflow.launchDir}/${params.metadataDir}"
egaCredentialsDir = "${workflow.launchDir}/${params.egaCredentialsDir}"

DATASETS = Channel.fromPath( "$dataDir/EGAD*", type: 'dir', checkIfExists: true )

DATASETS.map{ dir -> tuple( file(dir).getName(), dir ) }
    .set {KEYED_DATASETS}

process get_dbox_content {

    cache 'lenient'
    
    input:
        set val(dsId), file(dsPath) from KEYED_DATASETS

    output:
        set val(dsId), file('dbox_content') into DATASET_DBOXES

    """
    credentialsFile=$egaCredentialsDir/${dsId}.txt
    if [ ! -e \$credentialsFile ]; then
        echo "Credentials file missing at \$credentialsFile" 1>&2
        exit 1
    fi

    user=\$(grep "^user=" \$credentialsFile | sed s/user=//)
    password=\$(grep "^password=" \$credentialsFile | sed s/password=//)
    export ASPERA_SCP_PASS="\$password";   

    ascp --ignore-host-key -d -QTl 100m \${user}@xfer.crg.eu:dbox_content \$(pwd)/
    """
}

process make_metadata_table {

    conda 'r-base'

    cache 'deep'

    publishDir "$metadataDir/$dsId", mode: 'copy', overwrite: true

    input:
        set val(dsId), file(dbox) from DATASET_DBOXES

    output:
        file("${dsId}.merged.csv") into DATASET_CSVS

    """
    arrange_data.R $metadataDir $dataDir $dsId $dbox ${dsId}.merged.csv 
    """
}

DATASET_CSVS
    .splitCsv( header:true, sep:"\t" )
    .map{ row -> tuple(row['file'], row['ega_dataset_id'], row['biosample_id'], row['ega_run_id'], row['library_layout'], row['library_strategy'], row['dbox_path'], file(row['dbox_path']).getName()) }
    .set{
        DOWNLOAD_LIST
    }

process get_dbox_files {
    
    storeDir "$dataDir/$dsId/encrypted"
    
    cache 'lenient'
    
    maxForks 10
 
    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), val(dboxPath), val(dboxFileName) from DOWNLOAD_LIST

    output:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(dboxFileName) into ENCRYPTED_LINES

    """
    credentialsFile=$egaCredentialsDir/${dsId}.txt
    if [ ! -e \$credentialsFile ]; then
        echo "Credentials file missing at \$credentialsFile" 1>&2
        exit 1
    fi

    user=\$(grep "^user=" \$credentialsFile | sed s/user=//)
    password=\$(grep "^password=" \$credentialsFile | sed s/password=//)
    export ASPERA_SCP_PASS="\$password";   
 
    ascp --ignore-host-key -k 1 --partial-file-suffix=PART -QTl 100m \${user}@xfer.crg.eu:$dboxPath \$(pwd)/
    """
}

process decrypt {
    
    storeDir "$dataDir/$dsId/$libraryStrategy"

    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(encryptedFile) from ENCRYPTED_LINES

    output:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(fileName) into DECRYPTED_LINES

    """
    credentialsFile=$egaCredentialsDir/${dsId}.txt
    if [ ! -e \$credentialsFile ]; then
        echo "Credentials file missing at \$credentialsFile" 1>&2
        exit 1
    fi

    secret=\$(grep "^secret=" \$credentialsFile | sed s/secret=//)
    echo \$secret > secret.txt
    java -jar ${workflow.projectDir}/bin/decryptor.jar secret.txt $encryptedFile 
    """
}

DECRYPTED_LINES
    .into{
        DECRYPTED_LINES_FOR_CRAMS
        DECRYPTED_LINES_FOR_READMES
    }


DECRYPTED_LINES_FOR_CRAMS
    .filter{ row -> row[0].endsWith('.cram') }
    .set{
        CRAMS_FOR_FASTQS
    }

process cram_to_fastq {

    conda 'samtools'
    
    cache 'lenient'

    errorStrategy { task.attempt<=3 ? 'retry' : 'finish' }    
    memory { 2.GB * task.attempt }

    publishDir "$dataDir/$dsId/$libraryStrategy/fastq", mode: 'copy', overwrite: true

    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(cramFile) from CRAMS_FOR_FASTQS

    output:
        set val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file("${cramFile.baseName}_1.fastq.gz"), file("${cramFile.baseName}_2.fastq.gz") into FASTQS_FROM_CRAMS
    
    """
    export REF_CACHE=$dataDir/reference
    samtools collate $cramFile ${cramFile}.collated
    samtools fastq -F 2816 -c 6 -1 ${cramFile.baseName}_1.fastq.gz -2 ${cramFile.baseName}_2.fastq.gz ${cramFile}.collated.bam
    """
}

DECRYPTED_LINES_FOR_READMES
    .map{ row -> row[1] }
    .unique()
    .set{
        FINAL_DATASETS
    }

process add_readme {

    publishDir "$dataDir/$dsId", mode: 'copy', overwrite: true
    
    input:
        val(dsId) from FINAL_DATASETS
    
    output:
        file('README.md')

    """
    user=\$(whoami)
    pushd ${workflow.projectDir}
    remote=\$(git remote -v | head -n 1 | awk '{print \$2}')
    sha=\$(git rev-parse --verify HEAD)
    popd

    cp ${workflow.projectDir}/template_readme.md README.md
    sed -i "s/USER/\$user/g" README.md 
    sed -i "s#REMOTE#\$remote#g" README.md 
    sed -i "s/COMMIT/\$sha/g" README.md 
    """
}
