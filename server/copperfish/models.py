from django.db import models
from django.contrib.auth.models import User


class DataType (models.Model):
	name = models.CharField(null=False, max_length=64)
	description = models.TextField(null=False)
	date_created = models.DateField(auto_now_add = True)
	unit_test = models.ForeignKey('UnitTest')
	restricts = models.ForeignKey('self')



class UnitTest (models.Model):
	script_contents = models.TextField(null=False)



class DataSet (models.Model):
	user = models.ForeignKey(User) 
	#parent_dataset = models.ForeignKey('self')
	description = models.TextField(null=False)
	md5_checksum = models.CharField(null=False, max_length=64)
	filepath = models.FilePathField()
	date_created = models.DateField(auto_now_add=True)
	revision_id = models.ForeignKey('MethodRevision')
	contents = models.ForeignKey('DataSetContents')
	

class DataSetContents (models.Model):
	dataset_id = models.ForeignKey('DataSet')
	





class ParentDataSet (models.Model):
	dataset_id_parent = models.ForeignKey('DataSet', related_name='+')
	dataset_id = models.ForeignKey('DataSet', related_name='+')
	revision_id = models.ForeignKey('MethodRevision', related_name='+')


class Method (models.Model):
	name = models.TextField(null=False, unique=True)
	description = models.TextField(null=False)


class MethodRevision (models.Model):
	user = models.ForeignKey(User)
	method_id = models.ForeignKey('Method')
	revision_id_parent = models.ForeignKey('self')
	comment = models.TextField(null=False)
	revision_path = models.TextField(null=False)
	script_contents = models.TextField(null=False)
	revision_date = models.DateTimeField(null=False)
	

class MethodDependencies (models.Model):
	revision_id_parent = models.ForeignKey('MethodRevision', related_name='+')
	revision_id_needed = models.ForeignKey('MethodRevision', related_name='+')




