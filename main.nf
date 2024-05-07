#!/usr/bin/env nextflow

dataDir = "${workflow.launchDir}/${params.dataDir}"
metadataDir = "${workflow.launchDir}/${params.metadataDir}"
egaCredentialsDir = "${workflow.launchDir}/${params.egaCredentialsDir}"
fetchMode = params.fetchMode

DATASETS = Channel.fromPath( "$dataDir/EGAD*", type: 'dir', checkIfExists: true )

DATASETS.map{ dir -> tuple( file(dir).getName(), dir ) }
    .set {KEYED_DATASETS}

// aspera is to be deprecated, so we will use pyega3 to fetch the files
if (fetchMode == 'aspera'){
    process get_dbox_content {

        cache 'lenient'
        
        input:
            set val(dsId), file(dsPath) from KEYED_DATASETS

        output:
            set val(dsId), file('dbox_content') into AVAILABLE_FILES

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
}
else{
    process get_ega_file_listing {

        // This process is not strictly necessary, but it is useful to have a cached file listing.
        // Current versipn pf pyega3 can be provided directly with the dataset ID (EGAD*) and this will
        // directly download all relevant files without the need of storing them into a txt file
        
        cache 'lenient'

        conda 'pyega3>=5.2.0 python>=3.9.19'

        input:
            set val(dsId), file(dsPath) from KEYED_DATASETS
        
        output:
            set val(dsId), file('pyega3_file_listing.txt') into AVAILABLE_FILES

        """
        pyega3 -d -cf $egaCredentialsDir/ega.credentials files $dsId | grep 'File ID\\|EGAF' | sed 's/File /File_/g' | sed 's/Check /Check_/' | sed -e "s/ \\+/\\t/g" > pyega3_file_listing.txt
        """
    }
}

process make_metadata_table {

    conda 'r-base r-xml2 r-optparse'
    
    errorStrategy 'ignore'

    cache 'deep'

    publishDir "$metadataDir/$dsId", mode: 'copy', overwrite: true

    input:
        set val(dsId), file(file_listing) from AVAILABLE_FILES

    output:
        file("${dsId}.merged.csv") into DATASET_CSVS

    """
    # Do things slightly differently depending on where the file listing came from
    param='-y'
    if [ file_listing = 'dbox_content' ]; then
        param='-x'
    fi
    
    arrange_data.R -m $metadataDir -d $dataDir -i $dsId \$param $file_listing -o ${dsId}.merged.csv 
    """
}

if (fetchMode == 'aspera'){

    DATASET_CSVS
        .splitCsv( header:true, sep:"\t" )
        .map{ row -> tuple(row['file'], row['ega_dataset_id'], row['biosample_id'], row['ega_run_id'], row['library_layout'], row['library_strategy'], row['remote_path'], file(row['remote_path']).getName()) }
        .set{
            DOWNLOAD_LIST
        }

    process get_dbox_files {
        
        storeDir "$dataDir/$dsId/encrypted"

        errorStrategy { task.attempt<=3 ? 'retry' : 'ignore' }
        
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
}
else{
    
    DATASET_CSVS
        .splitCsv( header:true, sep:"\t" )
        .map{ row -> tuple(row['file'], row['ega_dataset_id'], row['biosample_id'], row['ega_run_id'], row['library_layout'], row['library_strategy'], row['remote_path'], file(row['remote_path']).getName(), row['file_id']) }
        .set{
            DOWNLOAD_LIST
        }

    process get_ega_files {
        
        storeDir "$dataDir/$dsId/$libraryStrategy"

        errorStrategy { task.attempt<=3 ? 'retry' : 'ignore' }
        
        cache 'lenient'

        conda 'pyega3>=5.2.0 python>=3.9.19'
        
        maxForks 10
     
        input:
            set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), val(remotePath), val(remoteFileName), val(fileId) from DOWNLOAD_LIST

        output:
            set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(fileName) into DECRYPTED_LINES

        """
        pyega3 -d -cf $egaCredentialsDir/ega.credentials fetch $fileId
        mv $fileId/$fileName .
        """

    }

}

DECRYPTED_LINES
    .into{
        DECRYPTED_LINES_FOR_CRAMS_BAMS
        DECRYPTED_LINES_FOR_READMES
    }


DECRYPTED_LINES_FOR_CRAMS_BAMS
    .filter{ row -> row[0].endsWith('.cram') | row[0].endsWith('.bam') }
    .set{
        CRAMS_BAMS_FOR_FASTQS
    }

process test_endedness {

    conda 'samtools'

    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(cramFile) from CRAMS_BAMS_FOR_FASTQS
    
    output:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(cramFile), stdout into ENDED_CRAMS_BAMS
   
    """
    n_paired_reads=\$(samtools view -c -f 1 $cramFile)
    if [ \$n_paired_reads = 0 ]; then
        echo -n SINGLE
    else
        echo -n PAIRED
    fi
    """ 
}

PAIRED = Channel.create()
UNPAIRED = Channel.create()

ENDED_CRAMS_BAMS.choice( UNPAIRED, PAIRED ) {a -> 
    a[7] == 'PAIRED' ? 1 : 0
}

// Paired bam to fastq

process paired_cram_bam_to_fastq {

    conda 'samtools'
    
    cache 'lenient'

    errorStrategy { task.attempt<=3 ? 'retry' : 'finish' }    
    memory { 2.GB * task.attempt }

    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(cramFile), val(end) from PAIRED

    output:
        set val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), val("${cramFile.baseName}"), file("${cramFile.baseName}_1.fastq.gz"), file("${cramFile.baseName}_2.fastq.gz") into FASTQS_FROM_PAIRED
    
    """
    export REF_CACHE=$dataDir/reference
    samtools collate $cramFile ${cramFile}.collated
    samtools fastq -F 2816 -c 6 -1 ${cramFile.baseName}_1.fastq.gz -2 ${cramFile.baseName}_2.fastq.gz ${cramFile}.collated.bam
    """
}

// Unpaired bam to fastq

process unpaired_cram_bam_to_fastq {

    conda 'samtools'
    
    publishDir "$dataDir/$dsId/$libraryStrategy/fastq", mode: 'copy', overwrite: true
    
    cache 'lenient'

    errorStrategy { task.attempt<=3 ? 'retry' : 'finish' }    
    memory { 2.GB * task.attempt }

    input:
        set val(fileName), val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file(cramFile), val(end) from UNPAIRED

    output:
        set val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), file("${cramFile.baseName}.fastq.gz") into FASTQS_FROM_UNPAIRED
    
    """
    export REF_CACHE=$dataDir/reference
    samtools collate $cramFile ${cramFile}.collated
    samtools fastq -F 2816 -c 6 ${cramFile}.collated.bam > ${cramFile.baseName}.fastq.gz 
    """
}

// Synchronise paired-end read files 

process synchronise_pairs {
  
    conda 'fastq-pair'
    
    publishDir "$dataDir/$dsId/$libraryStrategy/fastq", mode: 'copy', overwrite: true
  
    memory { 30.GB * task.attempt }

    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137 || task.attempt < 3 ? 'retry' : 'ignore' }
    maxRetries 3
    
    input:
        set val(dsId), val(biosampleId), val(egaRunId), val(libraryLayout), val(libraryStrategy), val(baseName), file('read1.fastq.gz'), file('read2.fastq.gz') from FASTQS_FROM_PAIRED

    output:
        set file( "${baseName}_1.fastq.gz" ), file("${baseName}_2.fastq.gz") into MATCHED_PAIRED_FASTQS

    beforeScript 'mkdir -p matched && mkdir -p unmatched'

    """
        zcat read1.fastq.gz > read1.fastq
        zcat read2.fastq.gz > read2.fastq
        fastq_pair read1.fastq read2.fastq

        gzip read1.fastq.paired.fq && mv read1.fastq.paired.fq.gz ${baseName}_1.fastq.gz
        gzip read2.fastq.paired.fq && mv read2.fastq.paired.fq.gz ${baseName}_2.fastq.gz

        rm -f read1.fastq read2.fastq
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
