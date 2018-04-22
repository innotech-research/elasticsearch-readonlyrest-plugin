/*
 *    This file is part of ReadonlyREST.
 *
 *    ReadonlyREST is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    ReadonlyREST is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with ReadonlyREST.  If not, see http://www.gnu.org/licenses/
 */

package tech.beshu.ror.es.security;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ExceptionsHelper;

import java.io.IOException;

// Code based on
// https://stackoverflow.com/questions/40949286/apply-lucene-query-on-bits
// https://github.com/apache/lucene-solr/blob/master/lucene/misc/src/java/org/apache/lucene/index/PKIndexSplitter.java#L127-L170

/*
 * @author Datasweet <contact@datasweet.fr>
 */
public final class DocumentFilterReader extends FilterLeafReader {

  private final Bits liveDocs;
  private final int numDocs;
  private final FieldLevelSecuritySettings flsSettings;

  private DocumentFilterReader(LeafReader reader, Query query, FieldLevelSecuritySettings flsSettings) throws IOException {
    super(reader);
    this.flsSettings = flsSettings;
    if (query != null) {
      final IndexSearcher searcher = new IndexSearcher(this);
      searcher.setQueryCache(null);
      final boolean needsScores = false;
      final Weight preserveWeight = searcher.createNormalizedWeight(query, needsScores);

      final int maxDoc = this.in.maxDoc();
      final FixedBitSet bits = new FixedBitSet(maxDoc);
      final Scorer preverveScorer = preserveWeight.scorer(this.getContext());
      if (preverveScorer != null) {
        bits.or(preverveScorer.iterator());
      }

      if (in.hasDeletions()) {
        final Bits oldLiveDocs = in.getLiveDocs();
        assert oldLiveDocs != null;
        final DocIdSetIterator it = new BitSetIterator(bits, 0L);
        for (int i = it.nextDoc(); i != DocIdSetIterator.NO_MORE_DOCS; i = it.nextDoc()) {
          if (!oldLiveDocs.get(i)) {
            bits.clear(i);
          }
        }
      }

      this.liveDocs = bits;
      this.numDocs = bits.cardinality();
    }
    else {
      this.liveDocs = reader.getLiveDocs();
      this.numDocs = in.numDocs();
    }

  }

  public static DocumentFilterDirectoryReader wrap(DirectoryReader in, Query filterQuery, FieldLevelSecuritySettings flsSettings) throws IOException {
    return new DocumentFilterDirectoryReader(in, filterQuery, flsSettings);
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {

    if (flsSettings == null || flsSettings.getFlsFields().isEmpty()) {
      super.document(docID, visitor);
      return;
    }

    super.document(docID, new StoredFieldVisitor() {
      @Override
      public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
        visitor.stringField(fieldInfo, value);
      }

      @Override
      public void intField(FieldInfo fieldInfo, int value) throws IOException {
        visitor.intField(fieldInfo, value);
      }

      @Override
      public void longField(FieldInfo fieldInfo, long value) throws IOException {
        visitor.longField(fieldInfo, value);
      }

      @Override
      public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        visitor.floatField(fieldInfo, value);
      }

      @Override
      public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        visitor.doubleField(fieldInfo, value);
      }

      @Override
      public boolean equals(Object obj) {
        return visitor.equals(obj);
      }

      @Override
      public int hashCode() {
        return visitor.hashCode();
      }

      @Override
      public String toString() {
        return visitor.toString();
      }

      @Override
      public Status needsField(FieldInfo fieldInfo) {
        if (flsSettings.isBlackList()) {
          return flsSettings.getFlsFields().contains(fieldInfo.name) ? Status.NO : Status.YES;
        }
        else {
          return flsSettings.getFlsFields().contains(fieldInfo.name) ? Status.YES : Status.NO;
        }
      }
    });
  }

  @Override
  public int numDocs() {
    return numDocs;
  }

  @Override
  public Bits getLiveDocs() {
    return liveDocs;
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return this.in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return this.in.getReaderCacheHelper();
  }

  private static final class DocumentFilterDirectorySubReader extends FilterDirectoryReader.SubReaderWrapper {
    private final Query query;
    private final FieldLevelSecuritySettings flsSettings;

    public DocumentFilterDirectorySubReader(Query filterQuery, FieldLevelSecuritySettings flsSettings) {
      this.query = filterQuery;
      this.flsSettings = flsSettings;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      try {
        return new DocumentFilterReader(reader, this.query, this.flsSettings);
      } catch (Exception e) {
        e.printStackTrace();
        throw ExceptionsHelper.convertToElastic(e);
      }
    }

  }

  public static final class DocumentFilterDirectoryReader extends FilterDirectoryReader {
    private final Query filterQuery;
    private final FieldLevelSecuritySettings flsSettings;

    DocumentFilterDirectoryReader(DirectoryReader in, Query filterQuery, FieldLevelSecuritySettings flsSettings) throws IOException {
      super(in, new DocumentFilterDirectorySubReader(filterQuery, flsSettings));
      this.filterQuery = filterQuery;
      this.flsSettings = flsSettings;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new DocumentFilterDirectoryReader(in, this.filterQuery, this.flsSettings);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return this.in.getReaderCacheHelper();
    }

  }
}