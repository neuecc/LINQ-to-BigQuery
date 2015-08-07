using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class StringTest
    {
        [TableName("publicdata:samples.github_timeline")]
        class Github
        {
            public string repository_url { get; set; }
        }

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void IgnoreCase()
        {
            Ctx.Select<int>(() => 100).Limit(100).IgnoreCase().ToString().Is(@"
SELECT
  100
LIMIT 100
IGNORE CASE".TrimSmart());

            // not ignore
            Ctx.Select<int>(() => 100).Limit(100).ToString().Is(@"
SELECT
  100
LIMIT 100".TrimSmart());
        }

        [TestMethod]
        public void Concat()
        {
            var j = "Java";
            var s = "Script";
            var t = "2";
            Ctx.Select<string>(() => BqFunc.Concat("Java", "Script", "2")).ToFlatSql().Is(@"SELECT CONCAT('Java', 'Script', '2')");
            Ctx.Select<string>(() => j + s + t).ToFlatSql().Is(@"SELECT (('Java' + 'Script') + '2')");
        }

        [TestMethod]
        public void Contains()
        {
            Ctx.Select<bool>(() => "aaa".Contains("bbb")).ToFlatSql().Is("SELECT 'aaa' CONTAINS 'bbb'");
        }

        [TestMethod]
        public void Contains2()
        {
            Ctx.Select<bool>(() => BqFunc.Contains("aaa", "bbb")).ToFlatSql().Is("SELECT 'aaa' CONTAINS 'bbb'");
        }

        [TestMethod]
        public void Left()
        {
            Ctx.Select<string>(() => BqFunc.Left("seatle", 3)).ToFlatSql().Is("SELECT LEFT('seatle', 3)");
        }

        [TestMethod]
        public void Length()
        {
            Ctx.Select<long>(() => BqFunc.Length("123456")).ToFlatSql().Is("SELECT LENGTH('123456')");
        }

        [TestMethod]
        public void Lower()
        {
            Ctx.Select<string>(() => BqFunc.Lower("aIuEo")).ToFlatSql().Is("SELECT LOWER('aIuEo')");
        }

        [TestMethod]
        public void Upper()
        {
            Ctx.Select<string>(() => BqFunc.Upper("aIuEo")).ToFlatSql().Is("SELECT UPPER('aIuEo')");
        }

        [TestMethod]
        public void PadLeft()
        {
            Ctx.Select<string>(() => BqFunc.PadLeft("1", 7, "?")).ToFlatSql().Is("SELECT LPAD('1', 7, '?')");
        }

        [TestMethod]
        public void PadRight()
        {
            Ctx.Select<string>(() => BqFunc.PadRight("1", 7, "?")).ToFlatSql().Is("SELECT RPAD('1', 7, '?')");
        }

        [TestMethod]
        public void Right()
        {
            Ctx.Select<string>(() => BqFunc.Right("kirkland", 4)).ToFlatSql().Is("SELECT RIGHT('kirkland', 4)");
        }

        [TestMethod]
        public void Split()
        {
            Ctx.From<Github>().Select(x => BqFunc.Split(x.repository_url)).Limit(1000).ToString().Is(@"
SELECT
  SPLIT([repository_url])
FROM
  [publicdata:samples.github_timeline]
LIMIT 1000".TrimSmart());


            Ctx.From<Github>().Select(x => BqFunc.Split(x.repository_url, "/")).Limit(1000).ToString().Is(@"
SELECT
  SPLIT([repository_url], '/')
FROM
  [publicdata:samples.github_timeline]
LIMIT 1000".TrimSmart());
        }

        [TestMethod]
        public void Substring()
        {
            Ctx.Select<string>(() => BqFunc.Substring("awesome", 3)).ToFlatSql().Is("SELECT SUBSTR('awesome', 3)");
            Ctx.Select<string>(() => BqFunc.Substring("awesome", -4, 4)).ToFlatSql().Is("SELECT SUBSTR('awesome', -4, 4)");
        }

        [TestMethod]
        public void InStr()
        {
            Ctx.Select<long>(() => BqFunc.InStr("awesome", "eso")).ToFlatSql().Is("SELECT INSTR('awesome', 'eso')");
        }

        [TestMethod]
        public void Trim()
        {
            Ctx.Select<string>(() => BqFunc.TrimLeft("  Say hello  ")).ToString().Is(@"SELECT
  LTRIM('  Say hello  ')");
            Ctx.Select<string>(() => BqFunc.TrimLeft("Say hello", "yaS")).ToFlatSql().Is("SELECT LTRIM('Say hello', 'yaS')");
            Ctx.Select<string>(() => BqFunc.TrimLeft("Say hello", " ySa")).ToString().Is(@"SELECT
  LTRIM('Say hello', ' ySa')");

            Ctx.Select<string>(() => BqFunc.TrimRight("  Say hello  ")).ToString().Is(@"SELECT
  RTRIM('  Say hello  ')");
            Ctx.Select<string>(() => BqFunc.TrimRight("Say hello", "leo")).ToFlatSql().Is("SELECT RTRIM('Say hello', 'leo')");
            Ctx.Select<string>(() => BqFunc.TrimRight("Say hello", " hloe")).ToString().Is(@"SELECT
  RTRIM('Say hello', ' hloe')");
        }
    }
}
