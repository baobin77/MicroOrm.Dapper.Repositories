using System;
using System.Collections.Generic;
using MicroOrm.Dapper.Repositories.SqlGenerator;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MicroOrm.Dapper.Repositories.UnitTest
{
    [TestClass]
    public class MySqlGenerator
    {
        private const SqlProvider SqlConnector = SqlProvider.MySQL;
        [TestMethod]
        public void Count()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetCount(null);
            Assert.AreEqual("SELECT COUNT(*) FROM `Users` WHERE `Users`.`Deleted` != 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public static void CountWithDistinct()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetCount(null, user => user.AddressId);
            Assert.AreEqual("SELECT COUNT(DISTINCT `Users`.`AddressId`) FROM `Users` WHERE `Users`.`Deleted` != 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public static void CountWithDistinctAndWhere()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetCount(x => x.PhoneId == 1, user => user.AddressId);
            Assert.AreEqual("SELECT COUNT(DISTINCT `Users`.`AddressId`) FROM `Users` WHERE `Users`.`PhoneId` = @PhoneId AND `Users`.`Deleted` != 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void BulkUpdate()
        {
            ISqlGenerator<Phone> userSqlGenerator = new SqlGenerator<Phone>(SqlConnector);
            var phones = new List<Phone>
            {
                new Phone { Id = 10, IsActive = true, Number = "111" },
                new Phone { Id = 10, IsActive = false, Number = "222" }
            };

            var sqlQuery = userSqlGenerator.GetBulkUpdate(phones);

            Assert.AreEqual("UPDATE DAB.Phones SET Number = @Number0, IsActive = @IsActive0 WHERE Id = @Id0; " +
                         "UPDATE DAB.Phones SET Number = @Number1, IsActive = @IsActive1 WHERE Id = @Id1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void BulkUpdate_QuoMarks()
        {
            ISqlGenerator<Phone> userSqlGenerator = new SqlGenerator<Phone>(SqlConnector, true);
            var phones = new List<Phone>
            {
                new Phone { Id = 10, IsActive = true, Number = "111" },
                new Phone { Id = 10, IsActive = false, Number = "222" }
            };

            var sqlQuery = userSqlGenerator.GetBulkUpdate(phones);

            Assert.AreEqual("UPDATE `DAB`.`Phones` SET `Number` = @Number0, `IsActive` = @IsActive0 WHERE `Id` = @Id0; " +
                         "UPDATE `DAB`.`Phones` SET `Number` = @Number1, `IsActive` = @IsActive1 WHERE `Id` = @Id1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void Insert_QuoMarks()
        {
            ISqlGenerator<Address> userSqlGenerator = new SqlGenerator<Address>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetInsert(new Address());

            Assert.AreEqual("INSERT INTO `Addresses` (`Street`, `CityId`) VALUES (@Street, @CityId); SELECT CONVERT(LAST_INSERT_ID(), SIGNED INTEGER) AS `Id`", sqlQuery.GetSql());
        }

        [TestMethod]
        public void IsNotNull()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetSelectAll(user => user.UpdatedAt != null);

            Assert.AreEqual("SELECT `Users`.`Id`, `Users`.`Name`, `Users`.`AddressId`, `Users`.`PhoneId`, `Users`.`OfficePhoneId`, `Users`.`Deleted`, `Users`.`UpdatedAt` FROM `Users` " +
                         "WHERE `Users`.`UpdatedAt` IS NOT NULL AND `Users`.`Deleted` != 1", sqlQuery.GetSql());
            //Assert.DoesNotContain("!= NULL", sqlQuery.GetSql());
        }


        [TestMethod]
        public void IsNotNullAnd()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetSelectAll(user => user.Name == "Frank" && user.UpdatedAt != null);

            Assert.AreEqual("SELECT `Users`.`Id`, `Users`.`Name`, `Users`.`AddressId`, `Users`.`PhoneId`, `Users`.`OfficePhoneId`, `Users`.`Deleted`, `Users`.`UpdatedAt` FROM `Users` " +
                         "WHERE `Users`.`Name` = @Name AND `Users`.`UpdatedAt` IS NOT NULL AND `Users`.`Deleted` != 1", sqlQuery.GetSql());
            //Assert.DoesNotContain("!= NULL", sqlQuery.GetSql());

            sqlQuery = userSqlGenerator.GetSelectAll(user => user.UpdatedAt != null && user.Name == "Frank");
            Assert.AreEqual("SELECT `Users`.`Id`, `Users`.`Name`, `Users`.`AddressId`, `Users`.`PhoneId`, `Users`.`OfficePhoneId`, `Users`.`Deleted`, `Users`.`UpdatedAt` FROM `Users` " +
                         "WHERE `Users`.`UpdatedAt` IS NOT NULL AND `Users`.`Name` = @Name AND `Users`.`Deleted` != 1", sqlQuery.GetSql());
            //Assert.DoesNotContain("!= NULL", sqlQuery.GetSql());
        }


        [TestMethod]
        public void SelectBetween()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetSelectBetween(1, 10, x => x.Id);

            Assert.AreEqual("SELECT `Users`.`Id`, `Users`.`Name`, `Users`.`AddressId`, `Users`.`PhoneId`, `Users`.`OfficePhoneId`, `Users`.`Deleted`, `Users`.`UpdatedAt` FROM `Users` " +
                         "WHERE `Users`.`Deleted` != 1 AND `Users`.`Id` BETWEEN '1' AND '10'", sqlQuery.GetSql());
        }

        [TestMethod]
        public void SelectById()
        {
            ISqlGenerator<Address> sqlGenerator = new SqlGenerator<Address>(SqlConnector, false);
            var sqlQuery = sqlGenerator.GetSelectById(1);
            Assert.AreEqual("SELECT Addresses.Id, Addresses.Street, Addresses.CityId FROM Addresses WHERE Addresses.Id = @Id LIMIT 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void SelectFirst()
        {
            ISqlGenerator<City> sqlGenerator = new SqlGenerator<City>(SqlConnector, false);
            var sqlQuery = sqlGenerator.GetSelectFirst(x => x.Identifier == Guid.Empty);
            Assert.AreEqual("SELECT Cities.Identifier, Cities.Name FROM Cities WHERE Cities.Identifier = @Identifier LIMIT 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void SelectFirst2()
        {
            ISqlGenerator<City> sqlGenerator = new SqlGenerator<City>(SqlConnector, false);
            var sqlQuery = sqlGenerator.GetSelectFirst(x => x.Identifier == Guid.Empty && x.Name == "");
            Assert.AreEqual("SELECT Cities.Identifier, Cities.Name FROM Cities WHERE Cities.Identifier = @Identifier AND Cities.Name = @Name LIMIT 1", sqlQuery.GetSql());
        }

        [TestMethod]
        public void SelectFirst_QuoMarks()
        {
            ISqlGenerator<City> sqlGenerator = new SqlGenerator<City>(SqlConnector, true);
            var sqlQuery = sqlGenerator.GetSelectFirst(x => x.Identifier == Guid.Empty);
            Assert.AreEqual("SELECT `Cities`.`Identifier`, `Cities`.`Name` FROM `Cities` WHERE `Cities`.`Identifier` = @Identifier LIMIT 1", sqlQuery.GetSql());
        }


        [TestMethod]
        public void SelectFirstWithDeleted()
        {
            ISqlGenerator<User> userSqlGenerator = new SqlGenerator<User>(SqlConnector, true);
            var sqlQuery = userSqlGenerator.GetSelectFirst(x => x.Id == 6);
            Assert.AreEqual("SELECT `Users`.`Id`, `Users`.`Name`, `Users`.`AddressId`, `Users`.`PhoneId`, `Users`.`OfficePhoneId`, `Users`.`Deleted`, `Users`.`UpdatedAt` FROM `Users` WHERE `Users`.`Id` = @Id AND `Users`.`Deleted` != 1 LIMIT 1", sqlQuery.GetSql());
        }
    }
}
