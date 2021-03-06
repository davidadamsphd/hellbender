package org.broadinstitute.hellbender.tools.recalibration;

import org.broadinstitute.hellbender.tools.recalibration.covariates.Covariate;
import org.broadinstitute.hellbender.tools.recalibration.covariates.StandardCovariateList;
import org.broadinstitute.hellbender.utils.collections.NestedIntegerArray;
import org.broadinstitute.hellbender.utils.recalibration.EventType;

import java.io.Serializable;
import java.util.*;

/**
 * Utility class to facilitate base quality score recalibration.
 */
public final class RecalibrationTables implements Serializable, Iterable<NestedIntegerArray<RecalDatum>> {
    private static final long serialVersionUID = 1L;
    private final int qualDimension;
    private final int eventDimension = EventType.values().length;
    private final int numReadGroups;

    //These two tables are special
    private final NestedIntegerArray<RecalDatum> readGroupTable;
    private final NestedIntegerArray<RecalDatum> qualityScoreTable;

    private final List<NestedIntegerArray<RecalDatum>> additionalTables;
    private final List<NestedIntegerArray<RecalDatum>> allTables;    //special + additional

    //bidirectional mappings
    private final Map<Covariate, NestedIntegerArray<RecalDatum>> covariateToTable;
    private final Map<NestedIntegerArray<RecalDatum>, Covariate> tableToCovariate;


    public RecalibrationTables(final StandardCovariateList covariates) {
        this(covariates, covariates.getReadGroupCovariate().maximumKeyValue() + 1);
    }

    public RecalibrationTables(StandardCovariateList covariates, final int numReadGroups) {
        this.additionalTables = new ArrayList<>();
        this.allTables = new ArrayList<>();
        this.covariateToTable = new LinkedHashMap<>();
        this.tableToCovariate = new LinkedHashMap<>();

        this.qualDimension = covariates.getQualityScoreCovariate().maximumKeyValue() + 1;
        this.numReadGroups = numReadGroups;

        //two special tables
        this.readGroupTable = new NestedIntegerArray<>(numReadGroups, eventDimension);
        allTables.add(readGroupTable);
        covariateToTable.put(covariates.getReadGroupCovariate(), readGroupTable);
        tableToCovariate.put(readGroupTable, covariates.getReadGroupCovariate());

        this.qualityScoreTable = makeQualityScoreTable();
        allTables.add(qualityScoreTable);
        covariateToTable.put(covariates.getQualityScoreCovariate(), qualityScoreTable);
        tableToCovariate.put(qualityScoreTable, covariates.getQualityScoreCovariate());

        //Non-special tables
        for (Covariate cov : covariates.getAdditionalCovariates()){
            final NestedIntegerArray<RecalDatum> table = new NestedIntegerArray<>(numReadGroups, qualDimension, cov.maximumKeyValue() + 1, eventDimension);
            additionalTables.add(table);
            allTables.add(table);
            covariateToTable.put(cov, table);
            tableToCovariate.put(table, cov);
        }
    }

    public NestedIntegerArray<RecalDatum> getTableForCovariate(Covariate cov) {
        return covariateToTable.get(cov);
    }

    public Covariate getCovariateForTable(NestedIntegerArray<RecalDatum> table) {
        return tableToCovariate.get(table);
    }

    public boolean isReadGroupTable(NestedIntegerArray<RecalDatum> table) {
        //Note: NestedIntegerArray does not implement equals so we use reference identity to check equality
        //to explicitly check identity (and future-proof against an equals method in NestedIntegerArray).
        return table == getReadGroupTable();
    }

    public boolean isQualityScoreTable(NestedIntegerArray<RecalDatum> table) {
        //Note: NestedIntegerArray does not implement equals so we use reference identity to check equality
        //to explicitly check identity (and future-proof against an equals method in NestedIntegerArray).
        return table == getQualityScoreTable();
    }

    public boolean isAdditionalCovariateTable(NestedIntegerArray<RecalDatum> table) {
        return additionalTables.contains(table);
    }

    public NestedIntegerArray<RecalDatum> getReadGroupTable() {
        return readGroupTable;
    }

    public NestedIntegerArray<RecalDatum> getQualityScoreTable() {
        return qualityScoreTable;
    }

    public int numTables() {
        return allTables.size();
    }

    @Override
    public Iterator<NestedIntegerArray<RecalDatum>> iterator() {
        return allTables.iterator();
    }

    /**
     * @return true if all the tables contain no RecalDatums
     */
    public boolean isEmpty() {
        for( final NestedIntegerArray<RecalDatum> table : allTables ) {
            if( !table.getAllValues().isEmpty() ) { return false; }
        }
        return true;
    }

    /**
     * Allocate a new quality score table, based on requested parameters
     * in this set of tables, without any data in it.  The return result
     * of this table is suitable for acting as a thread-local cache
     * for quality score values
     * @return a newly allocated, empty read group x quality score table
     */
    public NestedIntegerArray<RecalDatum> makeQualityScoreTable() {
        return new NestedIntegerArray<>(numReadGroups, qualDimension, eventDimension);
    }

    /**
     * Merge all of the tables from toMerge into into this set of tables
     */
    public void combine(final RecalibrationTables toMerge) {
        if ( numTables() != toMerge.numTables() )
            throw new IllegalArgumentException("Attempting to merge RecalibrationTables with different sizes");

        for ( int i = 0; i < numTables(); i++ ) {
            final NestedIntegerArray<RecalDatum> myTable = this.allTables.get(i);
            final NestedIntegerArray<RecalDatum> otherTable = toMerge.allTables.get(i);
            RecalUtils.combineTables(myTable, otherTable);
        }
    }

    //XXX this should not be accessible by index
    public NestedIntegerArray<RecalDatum> getTable(int index) {
        return allTables.get(index);
    }

    public List<NestedIntegerArray<RecalDatum>> getAdditionalTables() {
        return additionalTables;
    }
}
