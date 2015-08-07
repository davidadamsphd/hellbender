package org.broadinstitute.hellbender.engine.dataflow.datasources;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * RefAPIMetadata contains all of the information necessary to make a call to the Google Genomics API, currently
 * it's only the reference name and the table from reference name to id.
 */
public class RefAPIMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String referenceName;
    private final Map<String, String> referenceNameToIdTable;
    private final String apiKey;

    public RefAPIMetadata(String referenceName, Map<String, String> referenceNameToIdTable, String apiKey) {
        this.referenceName = referenceName;
        this.referenceNameToIdTable = Collections.unmodifiableMap(referenceNameToIdTable);
        this.apiKey = apiKey;

    }
    public RefAPIMetadata(String referenceName, Map<String, String> referenceNameToIdTable) {
        this(referenceName, referenceNameToIdTable, null);
    }


    public String getReferenceName() {
        return referenceName;
    }

    public Map<String, String> getReferenceNameToIdTable() {
        return referenceNameToIdTable;
    }

    public String getApiKey() {
        return apiKey;
    }
}
