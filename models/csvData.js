module.exports = (sequelize,DataTypes)=>{
    const CsvData = sequelize.define('CsvData',{
        id:{type:DataTypes.INTEGER, primaryKey:true},
        postId:DataTypes.INTEGER,
        name:DataTypes.STRING,
        email:DataTypes.STRING,
        body:DataTypes.TEXT
    },{
        timestamps:true,
        tableName:'csvdata'
    });
    return CsvData;
}