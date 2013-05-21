from django.db import models;
from django.contrib.auth.models import User;
from django.contrib.contenttypes.models import ContentType;
from django.contrib.contenttypes import generic;
from django.db.models.signals import pre_save, post_save;
from django.dispatch import receiver;

from django.core.exceptions import ValidationError;
from django.core.validators import MinValueValidator;

import operator;
# For auto-computing the MD5 of uploaded files.
import hashlib;

class Datatype(models.Model):
	# Note that none of these are nullable
	name = models.CharField(max_length=64)
	description = models.TextField()
	date_created = models.DateTimeField(auto_now_add = True)
	
	verification_script = models.FileField(upload_to='VerificationScripts')
	Python_type = models.CharField(max_length=64);
	
	# This is an asymmetric many-to-many relationship on itself;
	# this Datatype is the restrictor, and it can restrict many
	# other Datatypes.
	restricts = models.ManyToManyField('self', symmetrical=False,
	                                   related_name="restricted_by",
	                                   null=True, blank=True);

	# Implicitly defined:
	# - restricted_by: from field 'restricts'

	def __unicode__(self):
		return self.name;

class CompoundDatatype(models.Model):
	# This has a one-to-many relationship with its members
	
	# Implicitly defined:
	# - members: from CompoundDatatypeMember
	# - conforming_datasets: from Dataset

	def __unicode__(self):
		# Go through the members and stick together a string
		# representation of the compound datatype.

		string_rep = u"(";

		all_members = self.members.all();
		member_indices = [member.column_idx for member in all_members];
		members_with_indices = \
		    [(member_indices[i], all_members[i]) \
		     for i in range(len(all_members))];
		members_with_indices = sorted(members_with_indices,
		                              key=operator.itemgetter(0));
		
		for i, colIdx_and_member in enumerate(members_with_indices):
			colIdx, member = colIdx_and_member;
			string_rep += unicode(member);
			if i != len(members_with_indices) - 1:
				string_rep += ", ";
		string_rep += ")";
		
		return string_rep;

	# Coherence check: do all of its n members have consecutive indices,
	# 1 to n?
	def clean(self):
		column_indices = [];
		for member in self.members.all():
			column_indices += [member.column_idx];

		if sorted(column_indices) != range(1, self.members.count()+1):
			raise ValidationError(
					"Column indices are not consecutive starting from 1");


class CompoundDatatypeMember(models.Model):
	# In keeping with the suggestion in the Django docs, the
	# foreign key variable names are the names of the models,
	# lower-case.
	compounddatatype = models.ForeignKey(CompoundDatatype,
	                                     related_name="members");
	datatype = models.ForeignKey(Datatype);
	column_name = models.CharField(max_length=128);
	column_idx = \
		models.PositiveIntegerField(validators=[MinValueValidator(1)]);

	class Meta:
		unique_together = (("compounddatatype", "column_name"),
		                   ("compounddatatype", "column_idx"));

	def __unicode__(self):
		return u"{}: <{}> [{}]".format(self.column_idx,
									   unicode(self.datatype),
		                               self.column_name);


class Dataset(models.Model):
	user = models.ForeignKey(User);
	name = models.CharField(max_length=128);
	description = models.TextField();
	date_created = models.DateTimeField(auto_now_add=True);
	
	# What pipeline step it came from, and which output it was
	pipeline_step = models.ForeignKey("PipelineStep",
									  related_name="data_produced",
									  null=True, blank=True);
	pipeline_step_output_name = models.CharField(max_length=128,
												 blank=True);
	compounddatatype = \
	    models.ForeignKey(CompoundDatatype,
	                      related_name="conforming_datasets");

	parent_datasets = \
		models.ManyToManyField('self', related_name="descendent_datasets",
							   null=True, blank=True);
	dataset_file = models.FileField(upload_to="Datasets");
	MD5_checksum = models.CharField(max_length=64);

	# Implicitly defined:
	# - descendant_datasets: from field 'parent_datasets' (i.e. what Datasets
	#   are produced *from* this Dataset)

	def __unicode__(self):
		"""Create Unicode string representation of the dataset."""
		return "{} (created by {} on {})".format(self.name,
												 unicode(self.user),
												 self.date_created);
	
	def clean(self):
		"""If there is a file specified, fill in the MD5 checksum."""
		try:
			md5gen = hashlib.md5();
			md5gen.update(self.dataset_file.read());
			self.MD5_checksum = md5gen.hexdigest();
		except ValueError as e:
			print(e);
			print("No file found; setting MD5 checksum to the empty string.");
			self.MD5_checksum = "";
	

class CodeResource(models.Model):
	name = models.CharField(max_length=128);
	description = models.TextField();

	# Implicitly defined:
	# revisions - from CodeResourceRevision

	def __unicode__(self):
		return self.name;

class CodeResourceRevision(models.Model):
	coderesource = models.ForeignKey(CodeResource, related_name="revisions");	
		
	revision_name = models.CharField(max_length=128);
	revision_DateTime = models.DateTimeField(auto_now_add=True);
	revision_parent = models.ForeignKey('self', related_name="descendants",
										null=True, blank=True);
	revision_desc = models.TextField();

	content_file = models.FileField(upload_to="CodeResources", null=True,
									blank=True);
	MD5_checksum = models.CharField(max_length=64, blank=True);

	# Implicitly defined:
	# dependencies - from CodeResourceDependency
	# needed_by - also from CodeResourceDependency
	# descendants - from field 'revision_parent'

	def __unicode__(self):
		"""Create unicode string representation of model."""
		
		# This CodeResourceRevision may have no coderesource yet, e.g. if it is
		# being created from an inline on the CodeResource admin page.
		if not hasattr(self, "coderesource"):
			return u"[no code resource set] {}".format(self.revision_name);
		
		string_rep = self.coderesource.name + u" " + self.revision_name;
		return string_rep;

	def clean(self):
		"""If there is a file specified, fill in the MD5 checksum."""
		try:
			md5gen = hashlib.md5();
			md5gen.update(self.content_file.read());
			self.MD5_checksum = md5gen.hexdigest();
		except ValueError as e:
			#print(e);
			#print("No file found; setting MD5 checksum to the empty string.");
			self.MD5_checksum = "";

	

class CodeResourceDependency(models.Model):
	coderesourcerevision = models.ForeignKey(CodeResourceRevision,
											 related_name="dependencies");

	# This is the dependency; i.e. coderesourcerevision needs this to run
	requirement = models.ForeignKey(CodeResourceRevision,
	                                related_name="needed_by");
	
	# Where to put it (relative to the sandbox).
	# FIXME: should we use a FilePathField?
	where = models.CharField(max_length=100);

	def __unicode__(self):
		"""Represent as [x] requires [y] as [z]."""
		return u"{} requires {} as {}".format(
				unicode(self.coderesourcerevision),
				unicode(self.requirement),
				self.where);

			


# May 10, 2013: this is now in a one-to-many relationship with its members
class TransformationFamily(models.Model):
	name = models.CharField(max_length=128);
	description = models.TextField();

	def __unicode__(self):
		return self.name;

	class Meta:
		abstract = True;

class MethodFamily(TransformationFamily):
	# Implicitly defined:
	# members - from Method
	pass

class PipelineFamily(TransformationFamily):
	# Implicitly defined:
	# members - from Method
	pass


class Transformation(models.Model):
	revision_name = models.CharField(max_length=128);
	revision_DateTime = models.DateTimeField(auto_now_add = True);
	revision_desc = models.TextField();

	inputs = generic.GenericRelation("TransformationInput");
	outputs = generic.GenericRelation("TransformationOutput");

	# This is an abstract class:
	class Meta:
		abstract = True;

	def check_input_indices(self):
		"""Check coherence of the transformation inputs.

		We check that the inputs are properly numbered consecutively
		starting from 1, raising a ValidationError if not.
		"""
		input_nums = [];
		for curr_input in self.inputs.all():
			input_nums += [curr_input.dataset_idx];
		if sorted(input_nums) != range(1, self.inputs.count()+1):
			raise ValidationError(
					"Inputs are not consecutively numbered starting from 1");
		
	def check_output_indices(self):
		"""Check coherence of the transformation outputs.

		We check that the outputs are properly numbered consecutively
		starting from 1, raising a ValidationError if not.
		"""
		output_nums = [];
		for curr_output in self.outputs.all():
			output_nums += [curr_output.dataset_idx];
		if sorted(output_nums) != range(1, self.outputs.count()+1):
			raise ValidationError(
					"Outputs are not consecutively numbered starting from 1");

	def clean(self):
		"""Check coherence of the transformation inputs and outputs."""
		# Check that the inputs are numbered consecutively starting from 1.
		self.check_input_indices();
		self.check_output_indices();



# Methods and pipelines are manifestations of Transformation
# May 1, 2013: move revision_parent into Method and Pipeline from
# the abstract class Transformation because really we wouldn't have
# Methods descending from Pipelines or vice versa, so this makes more
# sense.
# May 10, 2013: change to have a one-to-many relationship with
# MethodFamily/PipelineFamily
class Method(Transformation):
	family = models.ForeignKey(MethodFamily, related_name="members");
		
	revision_parent = models.ForeignKey("self",
										related_name = "descendants",
										null=True, blank=True);
	# Note: this has to be an executable CodeResourceRevision
	driver = models.ForeignKey(CodeResourceRevision);

	# Implicitly defined:
	# descendants: from field 'revision_parent'
	
	def __unicode__(self):
		string_rep = u"Method {} {}".format("{}", self.revision_name);
		# If family is unset, e.g. if created from the family admin page
		if hasattr(self, "family"):
			string_rep = string_rep.format(unicode(self.family));
		else:
			string_rep = string_rep.format("[family unset]");
		return string_rep;

	def save(self, *args, **kwargs):
		"""When saving, set up inputs and outputs where appropriate.
		
		If this method has no inputs or outputs specified, but it does
		have a specified parent, then copy over the parent's inputs
		and outputs.  This must be done after first saving the method
		itself, otherwise we cannot add inputs and outputs to it.
		"""
		super(Method, self).save(*args, **kwargs);
		
		if self.revision_parent == None:
			return None;

		if self.inputs.count() + self.outputs.count() == 0:
			# Add the inputs and outputs in turn
			for parent_input in self.revision_parent.inputs.all():
				self.inputs.create(
						compounddatatype = parent_input.compounddatatype,
						dataset_name = parent_input.dataset_name,
						dataset_idx = parent_input.dataset_idx,
						min_row = parent_input.min_row,
						max_row = parent_input.max_row);
			for parent_output in self.revision_parent.outputs.all():
				self.outputs.create(
						compounddatatype = parent_output.compounddatatype,
						dataset_name = parent_output.dataset_name,
						dataset_idx = parent_output.dataset_idx,
						min_row = parent_output.min_row,
						max_row = parent_output.max_row);
				

class Pipeline(Transformation):
	family = models.ForeignKey(PipelineFamily, related_name="members");	
		
	revision_parent = models.ForeignKey("self",
										related_name = "descendants",
										null=True, blank=True);
	# Implicitly defined:
	# steps: from PipelineStep
	# descendants: from field 'revision_parent'
	# outmap: from PipelineOutputMapping

	# When defining a pipeline, we don't define the outputs; we define
	# outmap instead and during the clean stage the outputs are created.
	
	def __unicode__(self):
		string_rep = u"Pipeline {} {}".format("{}", self.revision_name);
		# If family is unset, e.g. if created from the family admin page
		if hasattr(self, "family"):
			string_rep = string_rep.format(unicode(self.family));
		else:
			string_rep = string_rep.format("[family unset]");
		return string_rep;

	def clean(self):
		"""Check coherence and finish setup of the pipeline.

		We check the following:
		 - the pipeline inputs are properly numbered consecutively
           starting from 1
		 - the steps are properly numbered consecutively starting from 1
		 - the inputs of each step are available to them when they are
           needed and they are of the type expected
		 - the outputs of the pipeline itself will be properly mapped
           to outputs generated by its steps.
		"""
		# Check that the inputs are numbered consecutively starting
		# from 1.  Note that at this point we don't necessarily care
		# about the outputs, but if they *are* set, then we check them
		# too.
		super(Pipeline, self).clean();

		all_steps = self.steps.all();
		# Check that the numbering of steps is fine.
		step_nums = [];
		for step in all_steps:
			# We don't clean each step; let's assume this is done already
			#step.full_clean();	
			step_nums += [step.step_num];
		if sorted(step_nums) != range(1, len(all_steps)+1):
			raise ValidationError(
					"Steps are not consecutively numbered starting from 1");

		# Check that the steps are coherent with each other.
 		for step in all_steps:
			# Check on the PipelineStepInputs
			for curr_in in step.inputs.all():
				input_requested = curr_in.provider_output_name;
				requested_from = curr_in.step_providing_input;
				feed_to_input = curr_in.transf_input_name;

				# Find the requested input; raise ValidationError on failure.
				req_input = None;
				if requested_from == 0:
					# i.e. this is one of the pipeline inputs
					try:	
						req_input = self.inputs.get(
								dataset_name=input_requested);
					except TransformationInput.DoesNotExist as e:
						raise ValidationError(
								"Pipeline does not have input \"{}\"".
								format(input_requested));	
				else:
					providing_step = all_steps[requested_from-1];
					try:
						req_input = \
							providing_step.transformation.outputs.get(
									dataset_name=input_requested);
					except TransformationOutput.DoesNotExist as e:
						raise ValidationError(
								"Transformation at step {} does not produce output \"{}\"".
								format(requested_from, input_requested));
						
					# Was this dataset deleted?
					if providing_step.outputs_to_delete.filter(
							dataset_to_delete=input_requested).count() != 0:
						raise ValidationError(
								"Input \"{}\" from step {} to step {} is deleted prior to request".
								format(input_requested, requested_from,
									   step.step_num));

				# Check that the requested input matches the expected prototype.
				# Note: we don't check for ValidationError because this was
				# already checked in the clean() step of PipelineStep.
				transf_input = step.transformation.inputs.get(
						dataset_name=feed_to_input);

				# FIXME: we're just going to enforce that transf_input
				# and req_input have the same CompoundDatatype, rather
				# than making sure that their CompoundDatatypes match;
				# is this too restrictive?				
				if req_input.compounddatatype != transf_input.compounddatatype:
					raise ValidationError(
							"Data fed to input \"{}\" of step {} does not have the expected CompoundDatatype".
							format(feed_to_input, step.step_num));

				provided_min_row = 0;
				required_min_row = 0;
				if req_input.min_row != None:
					providing_min_row = req_input.min_row;
				if transf_input.min_row != None:
					required_min_row = transf_input.min_row;
				if (provided_min_row < required_min_row):
					raise ValidationError(
							"Data fed to input \"{}\" of step {} may have too few rows".
							format(feed_to_input, step.step_num));
				
				provided_max_row = float("inf");
				required_max_row = float("inf");
				if req_input.max_row != None:
					providing_max_row = req_input.max_row;
				if transf_input.max_row != None:
					required_max_row = transf_input.max_row;
				if (provided_max_row > required_max_row):
					raise ValidationError(
							"Data fed to input \"{}\" of step {} may have too many rows".
							format(feed_to_input, step.step_num));

		# Check the output mappings, making sure the wiring is coherent.
		output_indices = [];
		for mapping in self.outmap.all():
			output_requested = mapping.provider_output_name;
			requested_from = mapping.step_providing_output;
			connect_to_output = mapping.output_name;
			output_indices += [mapping.output_idx];

			# Is the step number valid?
			if requested_from > len(all_steps):
				raise ValidationError(
						"Output requested from a non-existent step");	
			
			providing_step = all_steps[requested_from-1];
			req_output = None;
			try:
				req_output = providing_step.transformation.outputs.get(
						dataset_name=output_requested);
			except TransformationOutput.DoesNotExist as e:
				raise ValidationError(
						"Transformation at step {} does not produce output \"{}\"".
						format(requested_from, output_requested));

			# Was this output deleted by the step producing it?
			if providing_step.outputs_to_delete.filter(
					dataset_to_delete=output_requested).count() != 0:
				raise ValidationError(
						"Output \"{}\" from step {} is deleted prior to request".
						format(output_requested, requested_from));

		if sorted(output_indices) != range(1, self.outmap.count()+1):
			raise ValidationError(
					"Outputs are not consecutively numbered starting from 1");

			

	def save(self, *args, **kwargs):
		"""When saving, set up outputs as specified.

		This must be done after saving, because otherwise the manager for
		the calling instance's outputs will not have been set up.
		"""
		super(Pipeline, self).save(*args, **kwargs);

		# Nuke the outputs -- note that if we ever customize the delete()
		# method of TransformationOutput we'll need to change this.
		self.outputs.all().delete();

		all_steps = self.steps.all();

		# Recreate the outputs.
 		for mapping in self.outmap.all():
			output_requested = mapping.provider_output_name;
			requested_from = mapping.step_providing_output;
			connect_to_output = mapping.output_name;
			
			providing_step = all_steps[requested_from-1];
			# We don't check for ValidationError because we assume clean()
			# has already been called.
			req_output = providing_step.transformation.outputs.get(
					dataset_name=output_requested);
				
			self.outputs.create(compounddatatype=req_output.compounddatatype,
								dataset_name=connect_to_output,
								dataset_idx=mapping.output_idx,
								min_row=req_output.min_row,
								max_row=req_output.max_row);

 			

class PipelineStep(models.Model):
	pipeline = models.ForeignKey(Pipeline, related_name="steps");

	# Restrict the types to Method and Pipeline; note that the names
	# must be lower-case.
	content_type = models.ForeignKey(
			ContentType,
			limit_choices_to = {"model__in": ("method", "pipeline")});
	object_id = models.PositiveIntegerField();
	transformation = generic.GenericForeignKey("content_type", "object_id");

	# This is 1-based
	step_num = \
		models.PositiveIntegerField(validators=[MinValueValidator(1)]);
	
	# Implicitly defined:
	# inputs: from PipelineStepInput
	# outputs_to_delete: from PipelineStepDelete

	def __unicode__(self):
		# Default value for if pipeline is unset
		pipeline_name = "[no pipeline assigned]";	
		if hasattr(self, "pipeline"):
			pipeline_name = unicode(self.pipeline);
		return "{} step {}".format(pipeline_name, self.step_num);

	def recursive_pipeline_check(self, pipeline):
		"""Check if the specified pipeline occurs within this step."""
		contains_pipeline = False;	
		# Base case 1: the transformation is a method.
		if type(self.transformation) == Method:
			contains_pipeline = False;
		# Base case 2: the transformation equals the pipeline.
		elif self.transformation == pipeline:
			contains_pipeline = True;
		# Recursive case: go through all of the pipeline steps.
		else:
			transf_steps = self.transformation.steps.all();
			for step in transf_steps:
				step_contains_pipeline = step.recursive_pipeline_check(pipeline);
				if step_contains_pipeline:
					contains_pipeline = True;
		return contains_pipeline;

	def clean(self):
		"""Check coherence of this step of the pipeline.

		The checks we perform are as follows:
		 - Do the inputs come from prior steps?
		 - Do the inputs map correctly to the transformation at this step?
		 - Are the outputs marked for deletion ones that actually come from
		 this transformation?
		 - Does the transformation at this step contain the parent pipeline?
		Raises ValidationError if any checks are failed.
		"""
		# Check recursively to see if this step's transformation contains
		# the specified pipeline at all.
		if self.recursive_pipeline_check(self.pipeline):
			raise ValidationError("Step {} contains the parent pipeline".
								  format(self.step_num));
 			
		for curr_in in self.inputs.all():
			input_requested = curr_in.provider_output_name;
			requested_from = curr_in.step_providing_input;
			feed_to_input = curr_in.transf_input_name;
				
			# Does this input come from a step prior to this one?
			if requested_from >= self.step_num:
				raise ValidationError(
						"Input \"{}\" to step {} does not come from a prior step".
						format(input_requested, self.step_num));

			# Does the transformation at this step have an input named
			# feed_to_input?
			try:
				self.transformation.inputs.get(dataset_name=feed_to_input);
			except TransformationInput.DoesNotExist as e:
				raise ValidationError(
						"Transformation at step {} has no input named \"{}\"".
						format(self.step_num, feed_to_input));
 
		for curr_del in self.outputs_to_delete.all():
			to_del = curr_del.dataset_to_delete;

			# Check that to_del is one of the outputs of the current step's
			# Transformation.
			if self.transformation.outputs.\
				filter(dataset_name=to_del).count() == 0:
				raise ValidationError(
						"Transformation at step {} has no output named \"{}\"".
						format(self.step_num, to_del));


class PipelineStepInput(models.Model):
	pipelinestep = models.ForeignKey(PipelineStep, related_name = "inputs");
	transf_input_name = models.CharField(max_length=128);

	# PRE: step_providing_input < the step number of the PipelineStep this
	# input goes into
	# The coherence of the data here will be enforced at the Python level
	# (i.e. does this actually refer to a Dataset produced by the
	# Transformation at the specified step, etc.)
	step_providing_input = models.PositiveIntegerField();
	provider_output_name = models.CharField(max_length=128);

	def __unicode__(self):
		step_str = "[no pipeline step set]";
		if self.pipelinestep != None:
			step_str = unicode(self.pipelinestep);
		return "{}:{}".format(step_str, self.transf_input_name);	


class PipelineStepDelete(models.Model):
	pipelinestep = models.ForeignKey(PipelineStep,
	                                 related_name="outputs_to_delete");

	# Again, the coherence of this data will be enforced at the Python level
	# (i.e. does this actually refer to a Dataset that will be produced
	# by the Transformation at this step)
	dataset_to_delete = models.CharField(max_length=128);


class PipelineOutputMapping(models.Model):
	"""Specifies mapping of PipelineStep outputs to Pipeline outputs."""
	pipeline = models.ForeignKey(Pipeline, related_name="outmap");
	output_name = models.CharField(max_length=128);
	output_idx = models.PositiveIntegerField(
			validators=[MinValueValidator(1)]);

	# PRE: step_providing_output is an actual step of the pipeline
	# and provider_output_name actually refers to one of the outputs
	# at that step
	# The coherence of the data here will be enforced at the Python level
	step_providing_output = models.PositiveIntegerField(
			validators=[MinValueValidator(1)]);
	provider_output_name = models.CharField(max_length=128);

	def __unicode__(self):
		pipeline_name = "[no pipeline set]";
		if self.pipeline != None:
			pipeline_name = unicode(self.pipeline);

		return "{}:{} ({})".format(pipeline_name, self.output_idx,
								   self.output_name);


# Both TransformationInput and TransformationOutput look just like this,
# so in keeping with the DRY principle...
class TransformationXput(models.Model):
	content_type = models.ForeignKey(
			ContentType,
			limit_choices_to = {"model__in": ("method", "pipeline")});
	object_id = models.PositiveIntegerField();
	transformation = generic.GenericForeignKey("content_type", "object_id");
	
	compounddatatype = models.ForeignKey(CompoundDatatype);
	dataset_name = models.CharField(max_length=128);
	dataset_idx = models.PositiveIntegerField(
			validators=[MinValueValidator(1)]);
	
	# Nullable fields indicating that this dataset has
	# restrictions on how many rows it can have
	min_row = models.PositiveIntegerField(null=True, blank=True);
	max_row = models.PositiveIntegerField(null=True, blank=True);

	class Meta:
		abstract = True;

		unique_together = (("content_type", "object_id", "dataset_name"),
						   ("content_type", "object_id", "dataset_idx"));

	def __unicode__(self):
		return u"[{}]:{} {} {}".format(unicode(self.transformation),
									   self.dataset_idx,
									   unicode(self.compounddatatype),
									   self.dataset_name);

	
class TransformationInput(TransformationXput):
	# Implicitly defined:
	# transformations: from MapTransformationToInput
	pass

class TransformationOutput(TransformationXput):
	# Implicitly defined:
	# transformations: from MapTransformationToOutput
	pass
