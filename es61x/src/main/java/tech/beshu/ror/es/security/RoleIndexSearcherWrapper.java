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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.unboundid.util.args.ArgumentException;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import tech.beshu.ror.commons.Constants;
import tech.beshu.ror.commons.settings.BasicSettings;
import tech.beshu.ror.commons.shims.es.LoggerShim;
import tech.beshu.ror.commons.utils.FilterTransient;
import tech.beshu.ror.es.ESContextImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/*
 * @author Datasweet <contact@datasweet.fr>
 */
public class RoleIndexSearcherWrapper extends IndexSearcherWrapper {
  private final static Gson gson = new Gson();
  private final LoggerShim logger;
  private final Function<ShardId, QueryShardContext> queryShardContextProvider;
  private final ThreadContext threadContext;
  private final Boolean enabled;

  public RoleIndexSearcherWrapper(IndexService indexService, Settings s, Environment env) throws Exception {
    if (indexService == null) {
      throw new ArgumentException("Please provide an indexService");
    }
    Logger logger = Loggers.getLogger(this.getClass(), new String[0]);
    logger.info("Create new RoleIndexSearcher wrapper, [{}]", indexService.getIndexSettings().getIndex().getName());
    this.queryShardContextProvider = shardId -> indexService.newQueryShardContext(shardId.id(), null, null, null);
    this.threadContext = indexService.getThreadPool().getThreadContext();

    this.logger = ESContextImpl.mkLoggerShim(logger);
    BasicSettings baseSettings = BasicSettings.fromFileObj(this.logger, env.configFile().toAbsolutePath(), s);
    this.enabled = baseSettings.isEnabled();

  }

  @Override
  protected DirectoryReader wrap(DirectoryReader reader) {
    if (!this.enabled) {
      logger.warn("Document filtering not available. Return defaut reader");
      return reader;
    }

    // # FLS
    String flsHeader = null;
    FieldLevelSecuritySettings flsSettingsTransient = null;
    try {
      flsHeader = threadContext.getHeader(Constants.FIELDS_TRANSIENT);
      if (!Strings.isNullOrEmpty(flsHeader)) {
        String[] tmpSet = gson.fromJson(flsHeader, String[].class);
        flsSettingsTransient = new FieldLevelSecuritySettings(Optional.ofNullable(Sets.newHashSet(tmpSet)));
        if (flsSettingsTransient == null) {
          logger.debug("Couldn't extract fieldsTransient from threadContext.");
        }
      }
    } catch (Throwable t) {
      logger.error("cannot extract FLS fields from thread context. > " + flsHeader, t);
    }

    // # DLS
    String filterString = threadContext.getHeader(Constants.FILTER_TRANSIENT);
    FilterTransient filterTransient = FilterTransient.Deserialize(filterString);
    if (filterTransient == null) {
      logger.debug("Couldn't extract filterTransient from threadContext.");
    }

    if (filterTransient == null && flsSettingsTransient == null) {
      return reader;
    }

    String filter = null;
    if (filterString != null) {
      ShardId shardId = ShardUtils.extractShardId(reader);
      if (shardId == null) {
        throw new IllegalStateException(
            LoggerMessageFormat.format("Couldn't extract shardId from reader [{}]", new Object[] { reader }));
      }

      try {
        BooleanQuery.Builder boolQuery = new BooleanQuery.Builder();
        boolQuery.setMinimumNumberShouldMatch(1);
        QueryShardContext queryShardContext = this.queryShardContextProvider.apply(shardId);
        filter = filterTransient.getFilter();
        XContentParser parser = JsonXContent.jsonXContent.createParser(queryShardContext.getXContentRegistry(), filter);
        QueryBuilder queryBuilder = queryShardContext.parseInnerQueryBuilder(parser);
        ParsedQuery parsedQuery = queryShardContext.toFilter(queryBuilder);
        boolQuery.add(parsedQuery.query(), BooleanClause.Occur.SHOULD);
        ConstantScoreQuery theQuery = new ConstantScoreQuery(boolQuery.build());
        DirectoryReader wrappedReader = DocumentFilterReader.wrap(reader, theQuery, flsSettingsTransient);
        return wrappedReader;
      } catch (IOException e) {
        this.logger.error("Unable to setup document/field level security");
        throw ExceptionsHelper.convertToElastic(e);
      }
    }

    DirectoryReader wrappedReader = null;
    try {
      wrappedReader = DocumentFilterReader.wrap(reader, null, flsSettingsTransient);
    } catch (IOException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }
    return wrappedReader;
  }

  @Override
  protected IndexSearcher wrap(IndexSearcher indexSearcher) throws EngineException {
    return indexSearcher;
  }

}
