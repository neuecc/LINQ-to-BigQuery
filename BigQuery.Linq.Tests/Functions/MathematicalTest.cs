using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class MathematicalTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Abs()
        {
            Ctx.Select(() => BqFunc.Abs(-100L)).ToFlatSql().Is("SELECT ABS(-100)");
            Ctx.Select(() => BqFunc.Abs(-100.5)).ToFlatSql().Is("SELECT ABS(-100.5)");
        }

        [TestMethod]
        public void Trigonometric()
        {
            Ctx.Select(() => BqFunc.Acos(0.5)).ToFlatSql().Is("SELECT ACOS(0.5)");
            Ctx.Select(() => BqFunc.Acosh(2.4)).ToFlatSql().Is("SELECT ACOSH(2.4)");
            Ctx.Select(() => BqFunc.Asin(1.2)).ToFlatSql().Is("SELECT ASIN(1.2)");
            Ctx.Select(() => BqFunc.Asinh(2.3)).ToFlatSql().Is("SELECT ASINH(2.3)");
            Ctx.Select(() => BqFunc.Atan(2.3)).ToFlatSql().Is("SELECT ATAN(2.3)");
            Ctx.Select(() => BqFunc.Atanh(0.4)).ToFlatSql().Is("SELECT ATANH(0.4)");
            Ctx.Select(() => BqFunc.Cos(2.3)).ToFlatSql().Is("SELECT COS(2.3)");
            Ctx.Select(() => BqFunc.Cosh(2.3)).ToFlatSql().Is("SELECT COSH(2.3)");
            Ctx.Select(() => BqFunc.Sin(2.3)).ToFlatSql().Is("SELECT SIN(2.3)");
            Ctx.Select(() => BqFunc.Sinh(2.3)).ToFlatSql().Is("SELECT SINH(2.3)");
            Ctx.Select(() => BqFunc.Tan(2.3)).ToFlatSql().Is("SELECT TAN(2.3)");
            Ctx.Select(() => BqFunc.Tanh(2.3)).ToFlatSql().Is("SELECT TANH(2.3)");

             Ctx.Select(() => BqFunc.Atan2(2.4, 1.2)).ToFlatSql().Is("SELECT ATAN2(2.4, 1.2)");
        }

        [TestMethod]
        public void CeilFloorRound()
        {
            Ctx.Select(() => BqFunc.Ceil(15.123)).ToFlatSql().Is("SELECT CEIL(15.123)");
            Ctx.Select(() => BqFunc.Floor(15.123)).ToFlatSql().Is("SELECT FLOOR(15.123)");
            Ctx.Select(() => BqFunc.Round(15.123)).ToFlatSql().Is("SELECT ROUND(15.123)");
            Ctx.Select(() => BqFunc.Round(15.123, 2)).ToFlatSql().Is("SELECT ROUND(15.123, 2)");
        }

        [TestMethod]
        public void DegreeRadian()
        {
            Ctx.Select(() => BqFunc.Degrees(1.4)).ToFlatSql().Is("SELECT DEGREES(1.4)");
            Ctx.Select(() => BqFunc.Radians(80.2140913183152)).ToFlatSql().Is("SELECT RADIANS(80.2140913183152)");
        }

        [TestMethod]
        public void Log()
        {
            Ctx.Select(() => BqFunc.Log(1.4)).ToFlatSql().Is("SELECT LOG(1.4)");
            Ctx.Select(() => BqFunc.Log2(1.4)).ToFlatSql().Is("SELECT LOG2(1.4)");
            Ctx.Select(() => BqFunc.Log10(1.4)).ToFlatSql().Is("SELECT LOG10(1.4)");
        }

        [TestMethod]
        public void PI()
        {
            Ctx.Select(() => BqFunc.PI()).ToFlatSql().Is("SELECT PI()");
        }

        [TestMethod]
        public void Pow()
        {
            Ctx.Select(() => BqFunc.Pow(2, 4)).ToFlatSql().Is("SELECT POW(2, 4)");
        }

        [TestMethod]
        public void Random()
        {
            Ctx.Select(() => BqFunc.Random()).ToFlatSql().Is("SELECT RAND()");
            Ctx.Select(() => BqFunc.Random(1213)).ToFlatSql().Is("SELECT RAND(1213)");
        }

        [TestMethod]
        public void Sqrt()
        {
            Ctx.Select(() => BqFunc.Sqrt(2.4)).ToFlatSql().Is("SELECT SQRT(2.4)");
        }
    }
}
