module.exports = (sequelize,DataTypes)=>{
    const Comment = sequelize.define('Comment',{
        id:{type:DataTypes.INTEGER, primaryKey:true},
        postId:DataTypes.INTEGER,
        name:DataTypes.STRING,
        email:DataTypes.STRING,
        body:DataTypes.TEXT
    },{
        timestamps:true,
        tableName:'comment'
    });
    return Comment;
}