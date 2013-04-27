from django.db import models
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic

class Datatype(models.Model):
	# Note that none of these are nullable
	name = models.CharField(max_length=64)
	description = models.TextField()
	dateCreated = models.DateTimeField(auto_now_add = True)
	
	unitTest = models.FileField(upload_to='UnitTests')
	PythonType = models.CharField(max_length=64);
	
	# This is an asymmetric many-to-many relationship on itself;
	# this Datatype is the restrictor, and it can restrict many
	# other Datatypes.
	restricts = models.ManyToManyField('self', symmetrical=False,
					 related_name="restrictedBy");

	# Implicitly defined:
	# - restrictedBy: from field 'restricts'

class CompoundDatatype(models.Model):
	# This has a one-to-many relationship with its members
	
	# Implicitly defined:
	# - members: from CompoundDatatypeMember
	# - conformingDatasets: from Dataset
	pass


class CompoundDatatypeMember(models.Model):
	# In keeping with the suggestion in the Django docs, the
	# foreign key variable names are the names of the models,
	# lower-case.
	compounddatatype = models.ForeignKey(CompoundDatatype,
					     related_name="members");
	datatype = models.ForeignKey(Datatype);
	columnName = models.CharField(max_length=128);
	columnIdx = models.PositiveIntegerField();

class Transformation(models.Model):
	family = models.ManyToManyField("TransformationFamily");
	revisionDateTime = models.DateTimeField(auto_now_add = True);

	# In keeping with the instructions on
	# https://docs.djangoproject.com/en/dev/topics/db/models/
	# we need to specify the app and class to use when
	# defining related_name.
	#revisionParent = \
	#    models.ForeignKey('self', null=True,
	#		      related_name="%(app_label)s_%(class)s_descendants");
	
	# Since Transformation is an abstract class, we can't make a foreign
	# key to itself.  We have to use a generic relation.
	content_type = models.ForeignKey(ContentType);
	object_id = models.PositiveIntegerField();
	revisionParent = generic.GenericForeignKey('content_type', 'object_id');

	#descendants = generic.GenericRelation("Transformation");
	inputs = generic.GenericRelation("TransformationInput");
	outputs = generic.GenericRelation("TransformationOutput");
	
	revisionDesc = models.TextField();

	# Implicitly defined:
	# - %(app_label)s_%(class)s_descendants: from revisionParent
	#   (i.e. what Transformations this is a parent of)
	# - TransformationInput_set: from TransformationInput
	# - TransformationOutput_set: from TransformationOutput

	# This is an abstract class:
	class Meta:
		abstract = True;


# Both TransformationInput and TransformationOutput look just like this,
# so in keeping with the DRY principle...
class TransformationXput(models.Model):
	#transformation = \
	#    models.ForeignKey(Transformation,
	#		      related_name = "%(app_label)s_%(class)s_set");
	content_type = models.ForeignKey(ContentType);
	object_id = models.PositiveIntegerField();
	transformation = generic.GenericForeignKey("content_type", "object_id");
	
	compounddatatype = models.ForeignKey(CompoundDatatype);
	datasetName = models.CharField(max_length=128);
	# Nullable fields indicating that this dataset has
	# restrictions on how many rows it can have
	minRow = models.PositiveIntegerField(null=True);
	maxRow = models.PositiveIntegerField(null=True);

	class Meta:
		abstract = True;

	
class TransformationInput(TransformationXput):
	pass

class TransformationOutput(TransformationXput):
	pass


class TransformationFamily(models.Model):
	name = models.CharField(max_length=128);
	description = models.TextField();

	# Implicitly defined:
	# - Transformation_set: from Transformation (i.e. what Transformations
	#   belong to this TransformationFamily)


class Dataset(models.Model):
	user = models.ForeignKey(User);
	name = models.CharField(max_length=128);
	description = models.TextField();
	dateCreated = models.DateTimeField(auto_now_add=True);
	
	# What pipeline step it came from, and which output it was
	pipelineStep = models.ForeignKey("PipelineStep",
					 related_name="dataProduced");
	pipelineStepOutputName = models.CharField(max_length=128);
	compounddatatype = \
	    models.ForeignKey(CompoundDatatype,
			      related_name="conformingDatasets");

	parentDatasets = models.ManyToManyField('self',
					      related_name="descendentDatasets");
	datasetFile = models.FileField(upload_to="Datasets");
	MD5Checksum = models.CharField(max_length=64);

	# Implicitly defined:
	# - descendantDatasets: from field 'parentDatasets' (i.e. what Datasets
	#   are produced *from* this Dataset)
	


class CodeResourceFamily(models.Model):
	name = models.CharField(max_length=128);
	description = models.TextField();
	
	# Implicitly defined:
	# members: from CodeResource

class CodeResource(models.Model):
	family = models.ManyToManyField(CodeResourceFamily,
				      related_name="members");
	revisionName = models.CharField(max_length=128);
	revisionDateTime = models.DateTimeField(auto_now_add=True);
	revisionParent = models.ForeignKey('self', related_name="descendants");
	revisionDesc = models.TextField();

	contentFile = models.FileField(upload_to="CodeResources", null=True);
	MD5Checksum = models.CharField(max_length=64);

	# Implicitly defined:
	# dependencies - from CodeResourceDependency
	# neededBy - also from CodeResourceDependency
	# descendants - from field 'revisionParent'

class CodeResourceDependency(models.Model):
	coderesource = models.ForeignKey(CodeResource,
					 related_name="dependencies");
	# This is the dependency; i.e. coderesource needs this to run
	requirement = models.ForeignKey(CodeResource,
					related_name="neededBy");
	# Where to put it; we can use the parameters to this to restrict
	# where it can be put (i.e. so that it doesn't go in an intermediate
	# data directory).  FilePathField requires absolute paths, which is
	# not appropriate for us.
	# FIXME: should we go to FilePathField so that it does the
	# kinds of checking we need it to do when we enter a path (i.e.
	# make sure it's a reasonable filename?)
	where = models.CharField(max_length=100);


# Methods and pipelines are manifestations of Transformation
class Method(Transformation):
	# Note: this has to be an executable CodeResource
	driver = models.ForeignKey(CodeResource);


class Pipeline(Transformation):
	# Implicitly defined:
	# steps: from PipelineStep
	pass


class PipelineStep(models.Model):
	pipeline = models.ForeignKey(Pipeline, related_name="steps");

	#transformation = models.ForeignKey(Transformation);
	content_type = models.ForeignKey(ContentType);
	object_id = models.PositiveIntegerField();
	transformation = generic.GenericForeignKey("content_type",
						   "object_id");
	
	stepNum = models.PositiveIntegerField();
	
	# Implicitly defined:
	# inputs: from PipelineStepInput
	# outputsToDelete: from PipelineStepDelete


class PipelineStepInput(models.Model):
	pipelinestep = models.ForeignKey(PipelineStep, related_name = "inputs");

	# PRE: stepProvidingInput < the step number of the PipelineStep this
	# input goes into
	# The coherence of the data here will be enforced at the Python level
	# (i.e. does this actually refer to a Dataset produced by the
	# Transformation at the specified step)
	stepProvidingInput = models.PositiveIntegerField();
	inputDatasetName = models.CharField(max_length=128);

class PipelineStepDelete(models.Model):
	pipelinestep = models.ForeignKey(PipelineStep,
					 related_name = "outputsToDelete");

	# Again, the coherence of this data will be enforced at the Python level
	# (i.e. does this actually refer to a Dataset that will be produced
	# by the Transformation at this step)
	datasetToDelete = models.CharField(max_length=128);
