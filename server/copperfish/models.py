from django.db import models


class DataType (models.Model):
	name = models.CharField(null=False)
	description = models.TextField(null=False)
	date_created = models.DateField(auto_now_add = True)
	unit_test = models.ForeignKey('UnitTest')
	restricts = models.ForeignKey('self')



class Dataset (models.Model):
	parent_dataset_id = models.ForeignKey('self')
	description = models.TextField(null=False)
	md5_checksum = models.CharField(null=False)
	filepath = models.FilePathField()
	date_created = models.DateField(auto_now_add=True)
	user_id = models.ForeignKey('auth_user')
	revision_id = models.ForeignKey('MethodRevision')
	contents = models.ForeignKey('DataSetContents')
	

class DatasetContents (models.Model):
	dataset_id = models.ForeignKey('DataSet')
	
	


class UnitTest (models.Model):
	script_contents = models.TextField(null=False)




class ParentDataset (models.Model):
	dataset_id_parent = models.ForeignKey('Dataset')
	dataset_id = models.ForeignKey('Dataset')
	revision_id = models.ForeignKey('MethodRevision')


class Method (models.Model):
	name = models.TextField(null=False, unique=True)
	description = models.TextField(null=False)


class MethodRevision (models.Model):
	author = models.ForeignKey('auth_user')
	method_id = models.ForeignKey('Method')
	revision_id_parent = models.ForeignKey('self')
	comment = models.TextField(null=False)
	revision_path = models.TextField(null=False)
	script_contents = models.TextField(null=False)
	revision_date = models.DateTimeField(null=False)
	

class MethodDependencies (models.Model):
	revision_id_parent = models.ForeignKey('MethodRevision')
	revision_id_needed = models.ForeignKey('MethodRevision')



	

