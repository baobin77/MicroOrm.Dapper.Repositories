using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;


namespace MicroOrm.Dapper.Repositories.UnitTest
{
    [Table("Cities")]
    public class City
    {
        public Guid Identifier { get; set; }

        public string Name { get; set; }

    }
}