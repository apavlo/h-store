package edu.brown.workload;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.ExpressionType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PredicatePairs;

public class WorkloadAnalyzer {

	private Workload workload;
	private Database db;

	public WorkloadAnalyzer(Database catalog_db, Workload workload) {
		this.db = catalog_db;
		this.workload = workload;
	}

	/**
	 * @param args
	 */

	public static void main(String[] vargs) throws Exception {
		ArgumentsParser args = ArgumentsParser.load(vargs);
		args.require(ArgumentsParser.PARAM_CATALOG,
				ArgumentsParser.PARAM_WORKLOAD);

		WorkloadAnalyzer analyzer = new WorkloadAnalyzer(args.catalog_db,
				args.workload);
		int count = analyzer.getCountOfReferencesInInterval(20000);

		System.out.println("count is " + count);

	}

	public int getCountOfReferencesInInterval(int timeInterval) {
		int count = 0;
		for (TransactionTrace txn : workload) {
			Column outputColumnToRemember = null;
			Long rememberedTimestamp = null;

			for (QueryTrace query : txn.getQueries()) {
				Procedure proc = query.getCatalogProcedure(db);

				if (proc.getName().equals("InsertCallForwarding")) {
					Statement stmt = query.getCatalogItem(db);
					if (stmt.getName().equals("query1")) {
						outputColumnToRemember = stmt.getOutput_columns()
								.get(0);
						rememberedTimestamp = query.getStartTimestamp();
					}
					if (stmt.getName().equals("query2")) {
						PredicatePairs predicates = CatalogUtil
								.extractStatementPredicates(stmt, true);
						if (outputColumnToRemember != null) {
							Long difference = query.getStartTimestamp()
									- rememberedTimestamp;
							// System.out.println(query.getStartTimestamp() -
							// rememberedTimestamp);
							boolean predicateKeyEqualsPrevOutputColumn = predicates
									.get(0).getSecond().getName()
									.equals(outputColumnToRemember.getName());
							boolean isEqualToComparison = predicates.get(0)
									.getComparisonExp()
									.equals(ExpressionType.COMPARE_EQUAL);
							if (predicateKeyEqualsPrevOutputColumn
									&& isEqualToComparison
									&& difference < timeInterval)
								count++;
							outputColumnToRemember = null;
							rememberedTimestamp = null;
						}
					}

					/*
					 * SELECT S_ID FROM SUBSCRIBER WHERE SUB_NBR = ?
					 * 
					 * and SELECT SF_TYPE FROM SPECIAL_FACILITY WHERE S_ID = ?
					 */
				}

			}
		}

		return count;

	}

}
