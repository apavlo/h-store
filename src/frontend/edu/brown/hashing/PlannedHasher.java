/**
 * 
 */
package edu.brown.hashing;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

/**
 * @author aelmore Hasher that uses a planned partition plan, stored in the
 *         database catalog. This partition plan can change over time
 * 
 */
public class PlannedHasher extends DefaultHasher {

  /**
   * @param catalog_db
   * @param num_partitions
   */
  public PlannedHasher(CatalogContext catalogContext, int num_partitions) {
    super(catalogContext, num_partitions);
  }

  /**
   * @param catalog_db
   */
  public PlannedHasher(CatalogContext catalogContext) {
    super(catalogContext);
  }

  @Override
  public int hash(Object value) {
    throw new NotImplementedException("Hashing without Catalog not supported");
  }

  @Override
  public int hash(Object value, CatalogType catalogItem) {
    if (catalogItem instanceof Column) {
      // TODO
      //get Table
    } else if (catalogItem instanceof Procedure) {
      // TODO
    }
    throw new NotImplementedException("TODO");
  }

  @Override
  public int hash(Object value, int num_partitions) {
    throw new NotImplementedException("Hashing without Catalog not supported");
  }

}
