/*
1. 所有GetXX类方法都会返回(XX, error), 注意错误判断
2. 所有需要从后台加载数据的Model(Project, Instance, Table..)，在取属性前要调用XXX.Load()方法
*/

package odps
