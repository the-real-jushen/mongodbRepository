using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Jtext103.Repository;
using Jtext103.Repository.Interface;
using MongoDB.Driver.GeoJsonObjectModel;


namespace Jtext103.MongoDBProvider
{
    public class MongoDBRepository<T> : IRepository<T>
    {
        private MongoClient client;

        //use BsonDocument to increase the flexixbilty
        public MongoCollection<BsonDocument> Collection;

        private MongoDatabase db;
        private MongoServer mongoServer;

        public MongoDBRepository(string connection, string database, string col)
        {
            //here init the DB connection
            //notice the constructor has no argument, the DB provider may use a config filr to config the connection
            //but, it may be configed by the out side code, so refactorying of the core interface may needed.
            //string connectionString = @"mongodb://115.156.252.231:27017";
            client = new MongoClient(connection);
            mongoServer = client.GetServer();
            db = mongoServer.GetDatabase(database);
            Collection = db.GetCollection<BsonDocument>(col);
        }

        #region IReposotory<T> 成员

        #region build query
        public QueryObject<T> AppendQuery(QueryObject<T> exsitingQuery, Dictionary<string, object> queryDict, QueryLogic logic)
        {
            if (exsitingQuery.Query == null)
            {
                var query = new QueryDocument(queryDict);
                exsitingQuery.Query = query;
                return exsitingQuery;
            }
            else
            {
                var query1 = (IMongoQuery)exsitingQuery.Query;
                var query2 = new QueryDocument(queryDict);
                switch (logic)
                {
                    case QueryLogic.And:
                        exsitingQuery.Query = Query.And(query1, query2);
                        break;
                    case QueryLogic.Or:
                        exsitingQuery.Query = Query.Or(query1, query2);
                        break;
                    default:
                        throw new Exception("QueryLogic ERROR!");

                }
                return exsitingQuery;
            }
        }
        public QueryObject<T> AppendQuery(QueryObject<T> exsitingQuery, QueryOperator operatior, string field, object value, QueryLogic logic)
        {
            IMongoQuery query;
            switch (operatior)
            {
                case QueryOperator.Distinct:
                    throw new NotImplementedException();
                    break;
                case QueryOperator.Equal:
                    query = Query.EQ(field, BsonValue.Create(value));
                    break;
                case QueryOperator.Exist:
                    query = Query.Exists(field);
                    break;
                case QueryOperator.Greater:
                    query = Query.GT(field, BsonValue.Create(value));
                    break;
                case QueryOperator.GreaterEqual:
                    query = Query.GTE(field, BsonValue.Create(value));
                    break;
                case QueryOperator.In:
                    var values = value as IEnumerable<object>;
                    if (values == null)
                    {
                        throw new Exception("Value not supported, please cast to IEnumerable<object>");
                    }
                    var bsonValues = new List<BsonValue>();
                    foreach (var item in values)
                    {
                        bsonValues.Add(BsonValue.Create(item));   
                    }
                    query = Query.In(field, bsonValues);
                    break;
                case QueryOperator.Less:
                    query = Query.LT(field, BsonValue.Create(value));
                    break;
                case QueryOperator.LessEqual:
                    query = Query.LTE(field, BsonValue.Create(value));
                    break;
                case QueryOperator.Like:
                    query = Query.Matches(field, value.ToString());
                    break;
                case QueryOperator.Limit:
                    throw new NotImplementedException();
                    break;
                case QueryOperator.Not:
                    throw new NotImplementedException();
                    break;
                case QueryOperator.NotEqual:
                    query = Query.NE(field, BsonValue.Create(value));
                    break;
                case QueryOperator.Near:
                    {
                        //use the value to construct a point
                        //the value is longitude;latidute
                        var lng = double.Parse(value.ToString().Substring(0, value.ToString().IndexOf(";")));
                        var lat = double.Parse(value.ToString().Substring(value.ToString().IndexOf(";") + 1));
                        var point = GeoJson.Point(GeoJson.Geographic(lng, lat));
                        query = Query.Near(field, point);
                        break;
                    }
                case QueryOperator.NotIn:
                    throw new NotImplementedException();
                    break;
                case QueryOperator.Skip:
                    throw new NotImplementedException();
                    break;
                case QueryOperator.Sort:
                    throw new NotImplementedException();
                    break;
                default:
                    throw new Exception("QueryOperator ERROR!");
                    break;
            }
            if (exsitingQuery.Query == null)
            {
                exsitingQuery.Query = query;
                return exsitingQuery;
            }
            else
            {
                var query1 = (IMongoQuery)exsitingQuery.Query;
                var query2 = query;
                switch (logic)
                {
                    case QueryLogic.And:
                        exsitingQuery.Query = Query.And(query1, query2);
                        break;
                    case QueryLogic.Or:
                        exsitingQuery.Query = Query.Or(query1, query2);
                        break;
                    default:
                        throw new Exception("QueryLogic ERROR!");

                }
                return exsitingQuery;
            }
        }
        public QueryObject<T> AppendQuery(QueryObject<T> exsitingQuery, QueryObject<T> query, QueryLogic logic)
        {
            if (exsitingQuery.Query == null)
            {
                exsitingQuery.Query = query.Query;
                return exsitingQuery;
            }
            else
            {
                var query1 = (IMongoQuery)exsitingQuery.Query;
                var query2 = (IMongoQuery)query.Query;
                switch (logic)
                {
                    case QueryLogic.And:
                        exsitingQuery.Query = Query.And(query1, query2);
                        break;
                    case QueryLogic.Or:
                        exsitingQuery.Query = Query.Or(query1, query2);
                        break;
                    case QueryLogic.Not:
                        exsitingQuery.Query = Query.And(query1, Query.Not(query2));
                        break;
                    default:
                        throw new Exception("QueryLogic ERROR!");

                }
                return exsitingQuery;
            }
        }
        #endregion

        #region insert&save

        public void InsertMany(List<T> entity)
        {
            List<BsonDocument> bsonList = new List<BsonDocument>();
            entity.ForEach(t => bsonList.Add(t.ToBsonDocument()));
            Collection.InsertBatch(bsonList);
        }

        public void InsertOne(T entity)
        {
            Collection.Insert(entity);
        }

        public void SaveOne(T entity)
        {
            Collection.Save(entity);
        }

        #endregion insert&save

        #region delete

        public void Delete(T entity)
        {
            //collection.Remove(entity);
            throw new NotImplementedException();
        }

        public void Delete(QueryObject<T> queryObject)
        {
            Collection.Remove((IMongoQuery)(queryObject.Query));
        }

        public void Delete(Dictionary<string, object> queryObject)
        {
            var query = new QueryDocument(queryObject);
            Collection.Remove(query);
        }

        public void RemoveAll()
        {
            Collection.RemoveAll();
        }

        #endregion delete

        #region find
        public long FindCountOfResult(QueryObject<T> queryObject)
        {
            if (queryObject.Query == null)
            {
                return FindAllCount();
            }
            var results = Collection.FindAs<T>((IMongoQuery)(queryObject.Query)).Count();
            return results;
        }
        public IQueryable<T> FindAsQueryable(QueryObject<T> queryObject)
        {
            if (queryObject.Query == null)
            {
                return Collection.FindAllAs<T>().AsQueryable<T>();
            }
            var results = Collection.FindAs<T>((IMongoQuery)(queryObject.Query));
            return results.AsQueryable<T>();
        }
        public long FindAllCount()
        {
            return Collection.FindAllAs<T>().Count();
        }
        public IEnumerable<T> FindAll()
        {
            return Collection.FindAllAs<T>().AsEnumerable<T>();
        }
        public IEnumerable<T> FindAll(string sortByKey, bool isAscending)
        {
            IEnumerable<T> results;
            if (sortByKey == "" || sortByKey == null)
            {
                results = FindAll();
            }
            else
            {
                if (isAscending)
                {
                    results = Collection.FindAllAs<T>().SetSortOrder(SortBy.Ascending(sortByKey)).AsEnumerable<T>();
                }
                else
                {
                    results = Collection.FindAllAs<T>().SetSortOrder(SortBy.Descending(sortByKey)).AsEnumerable<T>();
                }
            }
            return results;
        }
        public IEnumerable<T> FindAll(int pageIndex, int pageSize)
        {
            IEnumerable<T> results;
            if (pageIndex == 0 || pageSize == 0)
            {
                results = FindAll();
            }
            else
            {
                results = Collection.FindAllAs<T>().SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize).AsEnumerable<T>();
            }
            return results;
        }
        public IEnumerable<T> FindAll(string sortByKey, bool isAscending, int pageIndex, int pageSize)
        {
            if ((sortByKey == "" || sortByKey == null) && (pageIndex == 0 || pageSize == 0))
            {
                return FindAll();
            }
            else if ((sortByKey == "" || sortByKey == null) && (pageIndex != 0 && pageSize != 0))
            {
                return FindAll(pageIndex, pageSize);
            }
            else if ((sortByKey != "" && sortByKey != null) && (pageIndex == 0 || pageSize == 0))
            {
                return FindAll(sortByKey, isAscending);
            }
            else
            {
                IEnumerable<T> results;
                if (isAscending)
                {
                    results = Collection.FindAllAs<T>().SetSortOrder(SortBy.Ascending(sortByKey)).SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                }
                else
                {
                    results = Collection.FindAllAs<T>().SetSortOrder(SortBy.Descending(sortByKey)).SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                }
                return results;
            }
        }
        public IEnumerable<T> Find(QueryObject<T> queryObject)
        {
            if (queryObject.Query == null)
            {
                return FindAll();
            }
            var results = Collection.FindAs<T>((IMongoQuery)(queryObject.Query));
            return results.AsEnumerable<T>();
        }
        public IEnumerable<T> Find(string name, object obj)
        {
            Dictionary<string, object> queryDict = new Dictionary<string, object>();
            queryDict.Add(name, obj.ToBsonDocument());
            var query = new QueryDocument(queryDict);
            var results = Collection.FindAs<T>(query);
            return results.AsEnumerable<T>();
        }
        /// <summary>
        /// 查找并返回排序结果（sortByKey为空字符串或者null则不排序）
        /// </summary>
        /// <param name="queryObject"></param>
        /// <param name="sortByKey">根据该参数排序,为空字符串或者null则不排序</param>
        /// <param name="isAscending">true,递增;false,递减</param>
        /// <returns></returns>
        public IEnumerable<T> Find(QueryObject<T> queryObject, string sortByKey, bool isAscending)
        {
            if (queryObject.Query == null)
            {
                return FindAll(sortByKey, isAscending);
            }
            IEnumerable<T> results;
            if (sortByKey == "" || sortByKey == null)
            {
                results = Find(queryObject);
            }
            else
            {
                var query = (IMongoQuery)(queryObject.Query);
                if (isAscending)
                {
                    results = Collection.FindAs<T>(query).SetSortOrder(SortBy.Ascending(sortByKey)).AsEnumerable<T>();
                }
                else
                {
                    results = Collection.FindAs<T>(query).SetSortOrder(SortBy.Descending(sortByKey)).AsEnumerable<T>();
                }
            }
            return results;
        }
        /// <summary>
        /// 查找并分页（pageIndex或pageSize为0，则返回全部结果，即不分页）
        /// </summary>
        /// <param name="queryObject"></param>
        /// <param name="pageIndex">页面号</param>
        /// <param name="pageSize">每页显示数量</param>
        /// <returns></returns>
        public IEnumerable<T> Find(QueryObject<T> queryObject, int pageIndex, int pageSize)
        {
            if (queryObject.Query == null)
            {
                return FindAll(pageIndex, pageSize);
            }
            IEnumerable<T> results;
            if (pageIndex == 0 || pageSize == 0)
            {
                results = Find(queryObject);
            }
            else
            {
                var query = (IMongoQuery)(queryObject.Query);
                results = Collection.FindAs<T>(query).SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize).AsEnumerable<T>();
            }
            return results;
        }
        /// <summary>
        /// 查找并排序分页
        /// </summary>
        /// <param name="queryObject"></param>
        /// <param name="sortByKey">根据该参数排序</param>
        /// <param name="isAscending">true,递增;false,递减</param>
        /// <param name="pageIndex">页面号</param>
        /// <param name="pageSize">每页显示数量</param>
        /// <returns></returns>
        public IEnumerable<T> Find(QueryObject<T> queryObject, string sortByKey, bool isAscending, int pageIndex, int pageSize)
        {
            if (queryObject.Query == null)
            {
                FindAll(sortByKey, isAscending, pageIndex, pageSize);
            }
            if ((sortByKey == "" || sortByKey == null) && (pageIndex == 0 || pageSize == 0))
            {
                return Find(queryObject);
            }
            else if ((sortByKey == "" || sortByKey == null) && (pageIndex != 0 && pageSize != 0))
            {
                return Find(queryObject, pageIndex, pageSize);
            }
            else if ((sortByKey != "" && sortByKey != null) && (pageIndex == 0 || pageSize == 0))
            {
                return Find(queryObject, sortByKey, isAscending);
            }
            else
            {
                IEnumerable<T> results;
                var query = (IMongoQuery)(queryObject.Query);
                if (isAscending)
                {
                    results = Collection.FindAs<T>(query).SetSortOrder(SortBy.Ascending(sortByKey)).SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                }
                else
                {
                    results = Collection.FindAs<T>(query).SetSortOrder(SortBy.Descending(sortByKey)).SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                }
                return results;
            }
        }

        /// <summary>
        /// 查找所有并排序
        ///（其中排序的参数有多个）
        /// </summary>
        /// <param name="sorts">排序的参数，key为field，value代表是否递增:true,递增;false,递减;前面的优先级高</param>
        /// <returns></returns>
        public IEnumerable<T> FindAll(List<KeyValuePair<string, bool>> sorts)
        {
            IEnumerable<T> results;
            if (sorts.Count() == 0 || sorts == null)
            {
                results = FindAll();
            }
            else
            {
                var source = Collection.FindAllAs<T>();
                for (int i = 0; i < sorts.Count(); i++)
                {
                    if (sorts[i].Value)
                    {
                        source = source.SetSortOrder(SortBy.Ascending(sorts[i].Key));
                    }
                    else
                    {
                        source = source.SetSortOrder(SortBy.Descending(sorts[i].Key));
                    }
                }
                results = source.AsEnumerable<T>();
            }
            return results;
        }

        /// <summary>
        /// 查找所有并排序分页
        /// （其中排序的参数有多个）
        /// </summary>
        /// <param name="sorts">排序的参数，key为field，value代表是否递增:true,递增;false,递减;前面的优先级高</param>
        /// <param name="pageIndex">页面号</param>
        /// <param name="pageSize">每页显示数量</param>
        /// <returns></returns>
        public IEnumerable<T> FindAll(List<KeyValuePair<string, bool>> sorts, int pageIndex, int pageSize)
        {

            if ((sorts.Count() == 0 || sorts == null) && (pageIndex == 0 || pageSize == 0))
            {
                return FindAll();
            }
            else if ((sorts.Count() == 0 || sorts == null) && (pageIndex != 0 && pageSize != 0))
            {
                return FindAll(pageIndex, pageSize);
            }
            else if ((sorts.Count() > 0 && sorts != null) && (pageIndex == 0 || pageSize == 0))
            {
                return FindAll(sorts);
            }
            else
            {
                IEnumerable<T> results;
                var source = Collection.FindAllAs<T>();
                for (int i = 0; i < sorts.Count(); i++)
                {
                    if (sorts[i].Value)
                    {
                        source = source.SetSortOrder(SortBy.Ascending(sorts[i].Key));
                    }
                    else
                    {
                        source = source.SetSortOrder(SortBy.Descending(sorts[i].Key));
                    }
                }
                results = source.SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                return results;
            }
        }

        /// <summary>
        /// 查找并排序
        /// （其中排序的参数有多个）
        /// </summary>
        /// <param name="queryObject">排序的参数，key为field，value代表是否递增:true,递增;false,递减;前面的优先级高</param>
        /// <param name="sorts"></param>
        /// <returns></returns>
        public IEnumerable<T> Find(QueryObject<T> queryObject, List<KeyValuePair<string, bool>> sorts)
        {
            if (queryObject.Query == null)
            {
                return FindAll(sorts);
            }
            IEnumerable<T> results;
            if (sorts.Count() == 0 || sorts == null)
            {
                results = Find(queryObject);
            }
            else
            {
                var query = (IMongoQuery)(queryObject.Query);
                var source = Collection.FindAs<T>(query);
                for (int i = 0; i < sorts.Count(); i++)
                {
                    if (sorts[i].Value)
                    {
                        source = source.SetSortOrder(SortBy.Ascending(sorts[i].Key));
                    }
                    else
                    {
                        source = source.SetSortOrder(SortBy.Descending(sorts[i].Key));
                    }
                }
                results = source.AsEnumerable<T>();
            }
            return results;
        }

        /// <summary>
        /// 查找并排序分页
        /// （其中排序的参数有多个）
        /// </summary>
        /// <param name="queryObject"></param>
        /// <param name="sorts">排序的参数，key为field，value代表是否递增:true,递增;false,递减;前面的优先级高</param>
        /// <param name="pageIndex">页面号</param>
        /// <param name="pageSize">每页显示数量</param>
        /// <returns></returns>
        public IEnumerable<T> Find(QueryObject<T> queryObject, List<KeyValuePair<string, bool>> sorts, int pageIndex, int pageSize)
        {
            if (queryObject.Query == null)
            {
                FindAll(sorts, pageIndex, pageSize);
            }
            if ((sorts.Count() == 0 || sorts == null) && (pageIndex == 0 || pageSize == 0))
            {
                return Find(queryObject);
            }
            else if ((sorts.Count() == 0 || sorts == null) && (pageIndex != 0 && pageSize != 0))
            {
                return Find(queryObject, pageIndex, pageSize);
            }
            else if ((sorts.Count() > 0 && sorts != null) && (pageIndex == 0 || pageSize == 0))
            {
                return Find(queryObject, sorts);
            }
            else
            {
                IEnumerable<T> results;
                var query = (IMongoQuery)(queryObject.Query);
                var source = Collection.FindAs<T>(query);
                for (int i = 0; i < sorts.Count(); i++)
                {
                    if (sorts[i].Value)
                    {
                        source = source.SetSortOrder(SortBy.Ascending(sorts[i].Key));
                    }
                    else
                    {
                        source = source.SetSortOrder(SortBy.Descending(sorts[i].Key));
                    }
                }
                results = source.SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize);
                return results;
            }
        }

        public T FindOneById(Guid id)
        {
            return Collection.FindOneByIdAs<T>(id);
        }

        #endregion

        #region update

        [Obsolete("use save one istead")]
        public void Update(T entity)
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Not in the IRepository, use it when you need to do complex update, and ref the MongoDB Driver
        /// </summary>
        /// <param name="queryObject"></param>
        /// <param name="updateObject"></param>
        public void Update(QueryObject<T> queryObject, IMongoUpdate updateObject)
        {
            var query = (IMongoQuery)(queryObject.Query);
            Collection.Update(query, updateObject);
        }



        public void Update(Dictionary<string, object> queryObject, Dictionary<string, object> updateObject)
        {
            var query = new QueryDocument(queryObject);
            var update = new UpdateDocument(updateObject);
            Collection.Update(query, update);
        }

        #endregion update

        #region register

        public void RegisterMap<T1>(IEnumerable<string> propertyList)
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(T1)))
            {
                BsonClassMap.RegisterClassMap<T1>(cm =>
                {
                    cm.AutoMap();
                    foreach (var prop in propertyList)
                    {
                        cm.MapField(prop);
                        //cm.MapProperty(prop);
                    }
                });
            }
        }

        public void RegisterMap<T1>()
        {
            if (!BsonClassMap.IsClassMapRegistered(typeof(T1)))
            {
                BsonClassMap.RegisterClassMap<T1>(cm =>
                {
                    cm.AutoMap();
                });
            }
        }

        #endregion register

        #endregion IReposotory<T> 成员

    }
}
