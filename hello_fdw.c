/*-------------------------------------------------------------------------
 *
 * hello_fdw.c
 * HelloWorld of foreign-data wrapper.
 *
 * written by Wataru Ikarashi <wikrsh@gmail.com>
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

typedef struct HelloFdwExecutionState
{
  int rownum;
} HelloFdwExecutionState;

/*
 * SQL functions
 */
extern Datum hello_fdw_handler(PG_FUNCTION_ARGS);
extern Datum hello_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hello_fdw_handler);
PG_FUNCTION_INFO_V1(hello_fdw_validator);

/*
 * FDW callback routines
 */
static void helloGetForeignRelSize(PlannerInfo *root,
                                   RelOptInfo *baserel,
                                   Oid foreigntableid);
static void helloGetForeignPaths(PlannerInfo *root,
                                 RelOptInfo *baserel,
                                 Oid foreigntableid);
static ForeignScan *helloGetForeignPlan(PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreigntableid,
                                        ForeignPath *best_path,
                                        List *tlist,
                                        List *scan_clauses);
static void helloExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void helloBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *helloIterateForeignScan(ForeignScanState *node);
static void helloReScanForeignScan(ForeignScanState *node);
static void helloEndForeignScan(ForeignScanState *node);
static bool helloAnalyzeForeignTable(Relation relation,
                                     AcquireSampleRowsFunc *func,
                                     BlockNumber *totalpages);

/*
 * Foreign-data wrapper handler function
 */
Datum
hello_fdw_handler(PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);
  
  fdwroutine->GetForeignRelSize = helloGetForeignRelSize;
  fdwroutine->GetForeignPaths = helloGetForeignPaths;
  fdwroutine->GetForeignPlan = helloGetForeignPlan;
  fdwroutine->ExplainForeignScan = helloExplainForeignScan;
  fdwroutine->BeginForeignScan = helloBeginForeignScan;
  fdwroutine->IterateForeignScan = helloIterateForeignScan;
  fdwroutine->ReScanForeignScan = helloReScanForeignScan;
  fdwroutine->EndForeignScan = helloEndForeignScan;
  fdwroutine->AnalyzeForeignTable = helloAnalyzeForeignTable;

  PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER
 * USER MAPPING or FOREIGN TABLE that uses hello_fdw.
 */
Datum
hello_fdw_validator(PG_FUNCTION_ARGS)
{
  /* no-op */
  PG_RETURN_VOID();
}

/*
 * helloGetForeignRelSize
 * Obtain relation size estimates for a foreign table
 */
static void
helloGetForeignRelSize(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid)
{
  /* hello_fdw returns 1 row */
  baserel->rows = 1;
  baserel->fdw_private = NULL;
}

/*
 * helloGetForeignPaths
 * Create Possible access paths for a scan on the foreign table
 */
static void
helloGetForeignPaths(PlannerInfo *root,
                     RelOptInfo *baserel,
                     Oid foreigntableid)
{
  add_path(baserel, 
           (Path*) create_foreignscan_path(root,
                                           baserel,
                                           baserel->rows,
                                           10, /* startup_cost */
                                           1000, /* total_cost */
                                           NIL, /* no pathkeys */
                                           NULL, /* no outer rel either */
                                           NIL));
}

/*
 * helloGetForeignPlan
 * Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
helloGetForeignPlan(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid,
                    ForeignPath *best_path,
                    List *tlist,
                    List *scan_clauses)
{
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist,
                          scan_clauses,
                          baserel->relid,
                          NIL,
                          best_path->fdw_private);
}

/*
 * helloExpainForeignScan
 * Produce extra output for EXPLAIN
 */
static void
helloExplainForeignScan(ForeignScanState *node,
                        ExplainState *es)
{
  ExplainPropertyText("Hello", "Hello Explain Value", es);
}

/*
 * helloBeginForeignScan
 */
static void
helloBeginForeignScan(ForeignScanState *node,
                      int eflags)
{
  HelloFdwExecutionState *hestate;

  /* Do nothing in EXPLAN */
  if(eflags & EXEC_FLAG_EXPLAIN_ONLY)
    return;

  hestate = (HelloFdwExecutionState *) palloc(sizeof(HelloFdwExecutionState));
  hestate->rownum = 0;
  
  node->fdw_state = (void *) hestate;
}

/*
 * helloIterateForeignScan
 * Generate next record and store it into the ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
helloIterateForeignScan(ForeignScanState *node)
{
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  Relation rel;
  AttInMetadata  *attinmeta;
  HeapTuple tuple;
  HelloFdwExecutionState *hestate = (HelloFdwExecutionState *) node->fdw_state;
  int i;
  int natts;
  char **values;

  if( hestate->rownum != 0 ){
    ExecClearTuple(slot);
    return slot;
  }

  rel = node->ss.ss_currentRelation;
  attinmeta = TupleDescGetAttInMetadata(rel->rd_att);
  
  natts = rel->rd_att->natts;
  values = (char **) palloc(sizeof(char *) * natts);

  for(i = 0; i < natts; i++ ){
    values[i] = "Hello,World";
  }

  tuple = BuildTupleFromCStrings(attinmeta, values);
  ExecStoreTuple(tuple, slot, InvalidBuffer, true);

  hestate->rownum++;

  return slot;
}

/*
 * helloReScanForeignScan
 * Rescan table, possibly with new parameters
 */
static void
helloReScanForeignScan(ForeignScanState *node)
{
  HelloFdwExecutionState *hestate = (HelloFdwExecutionState *) node->fdw_state;
  hestate->rownum = 0;
}

/*
 * helloEndForeignScan
 * Finish scanning foreign table and dispose objects used for this scan
 */
static void
helloEndForeignScan(ForeignScanState *node)
{
  /* nothing to do */
}

static bool
helloAnalyzeForeignTable(Relation relation,
                         AcquireSampleRowsFunc *func,
                         BlockNumber *totalpages)
{
  *totalpages = 1;
  return true;
}
